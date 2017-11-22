package main

import (
	pp "./pipeline"
	"bytes"
	"errors"
	"fmt"
	xp "gopkg.in/xmlpath.v2"
	y "gopkg.in/yaml.v2"
	"log"
	"os"
	"os/exec"
	fp "path/filepath"
	"strings"
	"time"
)

var launchTime = time.Now()

var UnexpectedName = errors.New("Name does not satisfy expected pattern.")

var cfg struct {
	Required  []string    `yaml:"Required"`
	Period    string      `yaml:"Period"`
	Prefix    string      `yaml:"Prefix"`
	WatchDir  string      `yaml:"WatchDir"`
	SubDir    string      `yaml:"SubDir"`
	OutputDir string      `yaml:"OutputDir"`
	Version   string      `yaml:"Version"`
	Pipeline  pp.Pipeline `yaml:"Pipeline"`
}

var pipeline pp.Pipeline

var watchDir = "/data"
var subDir = "result"
var prefix = "NPP"
var period = 30 * time.Second

type RequiredFile struct {
	Name string
	Path string
}

var required = []string{
	"SVM10",
	"GMTCO",
	"IICMO",
	"SVDNB",
	"SVM07",
	"SVM08",
	"SVM12",
	"SVM13",
	"SVM14",
	"SVM15",
	"SVM16",
}

var H5DumpBinary = "h5dump"
var DetectBinary = "viirs_detect"
var FitBinary = "viirs_fit"

var OutputDir = "/output"

var version = "v2.1"

func getId(file string) (string, error) {
	name := fp.Base(file)
	parts := strings.Split(name, "_")
	if len(parts) < 6 {
		return "", UnexpectedName
	}
	return strings.Join(parts[1:5], "_"), nil

}

func hasNight(file string) bool {
	h5dump := exec.Command(H5DumpBinary, "-x", "-A", file)
	out, err := h5dump.Output()
	if nil != err {
		log.Printf("WARN: H5Dump failed: %s\n", err.Error())
		return true
	}
	path, err := xp.Compile("//Attribute[contains(@Name, 'Ascending/Descending_Indicator')]/Data/DataFromFile")
	if nil != err {
		log.Printf("WARN: Failed to compile xpath query due to %s", err.Error())
		return true
	}
	root, err := xp.Parse(bytes.NewReader(out))
	if nil != err {
		log.Printf("WARN: Failed to retreive root node due to %s", err.Error())
		return true
	}
	iter := path.Iter(root)
	for iter.Next() {
		if strings.TrimSpace(iter.Node().String()) != "0" {
			return true
		}
	}
	return false
}

type File struct {
	Prefix       string
	Name         string
	Fullpath     string
	Size         int64
	LastModified time.Time
}

type FileGroup struct {
	Files         map[string]*File
	LastModified  time.Time
	Path          string
	Id            string // VIIRS qualified ID
	RequiredFound int
	HasNight      bool
}

func (fg *FileGroup) AnyChanged() (bool, error) {
	for _, f := range fg.Files {
		inf, err := os.Stat(f.Fullpath)
		if nil != err {
			return false, err
		}
		if inf.ModTime().After(f.LastModified) ||
			inf.Size() > f.Size {
			return true, nil
		}
	}
	return false, nil
}

func Process(fg *FileGroup) {
	for _, f := range fg.Files {
		if !hasNight(f.Fullpath) {
			log.Printf("No night data for %s ignoring", fg.Id)
			return
		}
		break
	}
	var gctx = make(map[string]interface{})
	gctx["Id"] = fg.Id
	gctx["Version"] = version
	gctx["OutputDir"] = OutputDir
	for _, f := range fg.Files {
		gctx[f.Prefix] = f.Fullpath
		gctx[f.Prefix+"_Name"] = strings.TrimSuffix(f.Name, fp.Ext(f.Name))
	}
	if err, str := pipeline.Exec(gctx); nil == err {
		log.Printf("INFO: Processing success %s", fg.Id)
	} else {
		log.Printf("ERROR: Pipeline failed with the following error: %v\n%s", err, str)
	}
}

// Keeps track of currently watched directories
type Watcher struct {
	root     string
	delay    time.Duration
	required []string
	found    map[string]*FileGroup
	ready    map[string]*FileGroup
}

func NewWatcher(root string, required []string, delay time.Duration) *Watcher {
	if 0 == delay {
		delay = 30 * time.Second
	}
	return &Watcher{
		root:     root,
		delay:    delay,
		required: required,
		found:    make(map[string]*FileGroup),
		ready:    make(map[string]*FileGroup),
	}
}

func (w *Watcher) isRequired(s string) (bool, string) {
	for i := range w.required {
		if strings.HasPrefix(s, w.required[i]) {
			return true, w.required[i]
		}
	}
	return false, ""
}

func (w *Watcher) Watch() error {
	print(w.root)
	for {
		fp.Walk(w.root, func(p string, inf os.FileInfo, err error) error {
			if nil != err {
				return err
			}
			name := inf.Name()
			if ".h5" != fp.Ext(p) {
				return nil
			}
			if req, _ := w.isRequired(name); !req {
				return nil
			}
			id, err := getId(name)
			if nil != err {
				log.Printf("Failed to extract id for a required file %s\n", p)
				return nil
			}
			if _, ok := w.ready[id]; ok {
				return nil
			}
			grp, ok := w.found[id]
			if !ok {
				if inf.ModTime().Before(launchTime) {
					return nil
				}
				dir, _ := fp.Split(p)
				w.found[id] = &FileGroup{
					Files: make(map[string]*File),
					Path:  dir,
					Id:    id,
				}
				grp = w.found[id]
			}
			if len(w.required) == grp.RequiredFound {
				if changed, err := grp.AnyChanged(); nil != err {
					log.Printf("Failed to check for group change %s", grp.Id)
				} else {
					if !changed {
						log.Printf("Group found %s", grp.Id)
						w.ready[grp.Id] = grp
						if grp.LastModified.After(launchTime) {
							Process(grp)
						} else {
							log.Printf("Group %s last modification time %s too old skipping.",
								grp.Id, grp.LastModified.Format("2006-01-02T15:04:05"))
						}
					}
				}
			}

			if inf.ModTime().After(grp.LastModified) {
				grp.LastModified = inf.ModTime()
			}

			f, ok := grp.Files[inf.Name()]
			if !ok {
				log.Printf("Found %s", inf.Name())
				grp.Files[inf.Name()] = &File{
					Prefix:       strings.Split(inf.Name(), "_")[0],
					Name:         inf.Name(),
					Fullpath:     p,
					Size:         inf.Size(),
					LastModified: inf.ModTime(),
				}
				f = grp.Files[inf.Name()]
				grp.RequiredFound += 1
			}

			f.LastModified = inf.ModTime()
			f.Size = inf.Size()

			return nil
		})
		<-time.After(w.delay)
	}
}

var noConfig = errors.New("No config file provided.")

func readConfig() error {
	if len(os.Args) < 2 {
		return noConfig
	}
	c, err := os.Open(os.Args[1])
	if nil != err {
		return err
	}
	defer c.Close()
	var bb bytes.Buffer
	if _, err = bb.ReadFrom(c); nil != err {
		return err
	}
	if err = y.Unmarshal(bb.Bytes(), &cfg); nil != err {
		return err
	}
	/*var cdec = json.NewDecoder(c)
	if err = cdec.Decode(&cfg); nil != err {
		return err
	}*/
	if 0 != len(cfg.Required) {
		required = cfg.Required
	}
	if "" != cfg.Period {
		if dur, err := time.ParseDuration(cfg.Period); nil == err {
			period = dur
		} else {
			return err
		}
	}
	if "" != cfg.Prefix {
		prefix = cfg.Prefix
	}
	if "" != cfg.WatchDir {
		watchDir = cfg.WatchDir
	}
	if "" != cfg.SubDir {
		subDir = cfg.SubDir
	}
	if "" != cfg.OutputDir {
		OutputDir = cfg.OutputDir
	}
	if "" != cfg.Version {
		version = cfg.Version
	}
	pipeline = cfg.Pipeline
	pipeline.Prepare()
	return nil
}

func main() {
	if err := readConfig(); nil != err {
		log.Printf("Failed to parse config: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Directory check period %v\n", period)
	var watcher = NewWatcher(cfg.WatchDir, required, period)
	watcher.Watch()
}
