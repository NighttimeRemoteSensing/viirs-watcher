package main

import (
	pp "./pipeline"
	"bytes"
	//"encoding/json"
	"errors"
	xp "gopkg.in/xmlpath.v2"
	y "gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

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

func getId(mainFile string) (string, error) {
	name := filepath.Base(mainFile)
	parts := strings.Split(name, "_")
	if len(parts) < 6 {
		return "", UnexpectedName
	}
	return strings.Join(parts[1:5], "_"), nil

}

func hasNight(mainFile string) bool {
	h5dump := exec.Command(H5DumpBinary, "-x", "-A", mainFile)
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

func Process(mainFile string, reqs []RequiredFile) {
	if !hasNight(mainFile) {
		log.Printf("No night data for %s ignoring", mainFile)
		return
	}
	id, err := getId(mainFile)
	if nil != err {
		log.Printf("ERROR: Processing of %s failed due to %s", mainFile, err.Error())
		return
	}
	var gctx = make(map[string]interface{})
	gctx["Id"] = id
	gctx["Version"] = version
	gctx["OutputDir"] = OutputDir
	for i := range reqs {
		gctx[reqs[i].Name] = reqs[i].Path
		var base = filepath.Base(reqs[i].Path)
		gctx[reqs[i].Name+"_Name"] = strings.TrimSuffix(base, filepath.Ext(base))
	}
	if err, str := pipeline.Exec(gctx); nil == err {
		log.Printf("INFO: Processing success %s", mainFile)
	} else {
		log.Printf("ERROR: Pipeline failed with the following error: %v\n%s", err, str)
	}
}

type Watcher struct {
	watched  map[string]struct{}
	chput    chan string
	chremove chan string
	chhas    chan struct {
		s   string
		res chan bool
	}
}

func (w *Watcher) Serve() {
	w.watched = make(map[string]struct{})
	w.chput = make(chan string)
	w.chremove = make(chan string)
	w.chhas = make(chan struct {
		s   string
		res chan bool
	})
	for {
		select {
		case name := <-w.chput:
			w.watched[name] = struct{}{}
		case name := <-w.chremove:
			delete(w.watched, name)
		case reqres := <-w.chhas:
			_, ok := w.watched[reqres.s]
			reqres.res <- ok
		}
	}
}

func (w *Watcher) Put(name string) {
	w.chput <- name
}

func (w *Watcher) Remove(name string) {
	w.chremove <- name
}

func (w *Watcher) Has(name string) bool {
	var reqres struct {
		s   string
		res chan bool
	}
	reqres.res = make(chan bool)
	w.chhas <- reqres
	return <-reqres.res
}

// Keeps track of currently watched directories
var watcher Watcher

func isRequired(s string) (bool, string) {
	for i := range required {
		if strings.HasPrefix(s, required[i]) {
			return true, required[i]
		}
	}
	return false, ""
}

func hasChanged(f string, to time.Duration) bool {
	finfo, err := os.Stat(f)
	if nil != err {
		log.Printf("Failed to stat file %s", f)
		return false
	}
	osmt := finfo.ModTime()
	<-time.After(to)
	finfo, err = os.Stat(f)
	if nil != err {
		log.Printf("Failed to stat file %s", f)
		return false
	}
	if osmt.Equal(finfo.ModTime()) {
		return false
	}
	log.Printf("DEBUG: ModTime has changed from %v to %v for %s in %v", osmt, finfo.ModTime(), f, to)
	return true
}

func watchSub(dir string) {
	log.Printf("Watching subdirectory %s", dir)
	found := 0
	marked := make(map[string]bool)
	var mainFile string
	var requiredFiles []RequiredFile
	for found != len(required) {
		finfos, err := ioutil.ReadDir(dir)
		if nil != err {
			log.Printf("Failed to read subdirectory %s", dir)
		}
		for _, f := range finfos {
			if rqrd, prefix := isRequired(f.Name()); rqrd {
				_, ok := marked[f.Name()]
				requiredFiles = append(requiredFiles, RequiredFile{
					Name: prefix,
					Path: filepath.Join(dir, f.Name())})
				if !ok && !hasChanged(filepath.Join(dir, f.Name()), 3*time.Second) {
					found += 1
					log.Printf("Required file %s found", f.Name())
					marked[f.Name()] = true
					if strings.HasPrefix(f.Name(), required[0]) {
						mainFile = filepath.Join(dir, f.Name())
					}
				}
			}
		}
		<-time.After(period)
	}
	Process(mainFile, requiredFiles)
}

func watchRoot(dir string, since time.Time) time.Time {
	maxTime := since
	finfos, err := ioutil.ReadDir(dir)
	if nil != err {
		log.Printf("Failed to read directory %s", dir)
	}

	for _, finfo := range finfos {
		if !strings.HasPrefix(finfo.Name(), prefix) {
			// Probably other sattelite data or something
			continue
		}

		subdir := filepath.Join(dir, finfo.Name(), subDir)
		subinfo, err := os.Stat(subdir)
		if os.ErrNotExist == err {
			// Directory containing actual files not yet created
			continue
		}

		if !subinfo.ModTime().After(since) {
			// Too old
			continue
		}

		if watcher.Has(subdir) {
			// Already watching it
			continue
		}

		// After all the checks it seems like this is one of the directories we
		// were looking for
		watcher.Put(subdir)

		go func(subdir string) {
			for hasChanged(subdir, 30*time.Second) {
				log.Printf("DEBUG: sub directory %s has been modified ignoring it for now.\n", subdir)
			}
			// Finally directory has not been changing for quite a while, thus we will scan it
			// and process if everything is in order
			watchSub(subdir)
			watcher.Remove(subdir)
		}(subdir)

		if subinfo.ModTime().After(maxTime) {
			// update time milestone
			maxTime = subinfo.ModTime()
		}
	}
	return maxTime
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
	go watcher.Serve()
	checkTime := time.Now()
	for {
		checkTime = watchRoot(watchDir, checkTime)
		<-time.After(period)
	}
}
