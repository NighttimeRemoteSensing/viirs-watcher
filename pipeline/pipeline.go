package pipeline

import (
	"bytes"
	"errors"
	"fmt"
	y "gopkg.in/yaml.v2"
	"io"
	"log"
	"os/exec"
	"strconv"
	tt "text/template"
)

const lDelim = "(("
const rDelim = "))"

var NotAString = errors.New("Variable name not a string.")

type Variable struct {
	Name       string      `yaml:"Name"`
	Value      interface{} `yaml:"Value"`
	expression *tt.Template
}

type Variables []Variable

func (v *Variables) UnmarshalYAML(f func(interface{}) error) error {
	var s y.MapSlice
	if err := f(&s); nil != err {
		return err
	}
	var vars = make(Variables, len(s))
	for i := range s {
		k, ok := s[i].Key.(string)
		if !ok {
			return NotAString
		}
		vars[i] = Variable{
			Name:  k,
			Value: s[i].Value,
		}
	}
	*v = vars
	return nil
}

func (v *Variable) Prepare() error {
	val, ok := v.Value.(string)
	if !ok {
		return nil
	}
	t, err := tt.New(v.Name).Delims(lDelim, rDelim).Parse(val)
	if nil != err {
		return err
	}
	v.expression = t
	return nil
}

func (v *Variable) Eval(ctx map[string]interface{}) (interface{}, error) {
	var bb bytes.Buffer
	if nil == v.expression {
		return v.Value, nil
	}
	if err := v.expression.Execute(&bb, ctx); nil != err {
		return nil, err
	}
	var str = bb.String()
	if b, err := strconv.ParseBool(str); nil == err {
		return b, nil
	} else if i, err := strconv.ParseInt(str, 10, 64); nil == err {
		return i, nil
	} else if f, err := strconv.ParseFloat(str, 64); nil == err {
		return f, nil
	}
	return str, nil
}

/* Step variables may overwrite Pipeline variables */
type Step struct {
	Name      string    `yaml:"Name"`
	Variables Variables `yaml:"Variables"`
	Command   string    `yaml:"Command"`
	command   *tt.Template
}

func (s *Step) UnmarshalYAML(f func(interface{}) error) error {
	var str string
	if nil == f(&str) {
		s.Command = str
		return nil
	}
	return f(s)
}

func (s *Step) Prepare() error {
	for i := range s.Variables {
		if err := s.Variables[i].Prepare(); nil != err {
			return err
		}
	}
	c, err := tt.New(s.Name).Delims(lDelim, rDelim).Parse(s.Command)
	if nil != err {
		return err
	}
	s.command = c
	return nil
}

func (s *Step) EvalVariables(ctx map[string]interface{}) (map[string]interface{}, error) {
	nctx := make(map[string]interface{})
	for k, v := range ctx {
		nctx[k] = v
	}
	for i := range s.Variables {
		if val, err := s.Variables[i].Eval(ctx); nil == err {
			nctx[s.Variables[i].Name] = val
		} else {
			return nil, err
		}
	}
	for k := range ctx {
		ctx[k] = nctx[k]
	}
	return nctx, nil
}

func (s *Step) Exec(ctx map[string]interface{}) (error, string) {
	nctx, err := s.EvalVariables(ctx)
	if nil != err {
		return err, ""
	}
	var buf bytes.Buffer
	if err = s.command.Execute(&buf, nctx); nil != err {
		return err, ""
	}
	var r rune
	var acc bytes.Buffer
	var split []string
	var state int
	const (
		DEFAULT = 0
		IN_SQ   = 1
		IN_DQ   = 2
		ESCAPED = 3
	)
	var wrap = func() {
		if 0 != acc.Len() {
			split = append(split, acc.String())
		}
		acc.Reset()
	}
	for ; nil == err; r, _, err = buf.ReadRune() {
		if r == '\uFFFD' || r == 0 {
			continue
		}
		switch state {
		case DEFAULT:
			switch r {
			case ' ', '\n', '\t', '\r':
				wrap()
			case '\'':
				state = IN_SQ
			case '"':
				state = IN_DQ
			case '\\':
				state = ESCAPED
			default:
				acc.WriteRune(r)
			}
		case IN_SQ:
			switch r {
			case '\'':
				wrap()
			default:
				acc.WriteRune(r)
			}
		case IN_DQ:
			switch r {
			case '"':
				wrap()
			default:
				acc.WriteRune(r)
			}
		case ESCAPED:
			acc.WriteRune(r)
		}
	}
	if io.EOF != err {
		return err, ""
	} else {
		wrap()
	}
	var cmd string
	var args []string
	if 0 != len(split) {
		cmd = split[0]
		args = split[1:]
	}
	log.Printf("DEBUG: Invoking command %s with arguments %v\n", cmd, args)
	out, err := exec.Command(cmd, args...).Output()
	var res = string(out)
	if err, ok := err.(*exec.ExitError); ok {
		res = fmt.Sprintf("%s\n%s", res, err.Stderr)
	}
	return err, res
}

/* Apart from variables defined, there is also globaly available Id variable */
type Pipeline struct {
	Variables Variables `yaml:"Variables"`
	Steps     []Step    `yaml:"Steps"`
}

func (p *Pipeline) Prepare() error {
	for i := range p.Variables {
		if err := p.Variables[i].Prepare(); nil != err {
			return err
		}
	}
	for i := range p.Steps {
		if err := p.Steps[i].Prepare(); nil != err {
			return err
		}
	}
	return nil
}

func (p *Pipeline) EvalVariables(ctx map[string]interface{}) (map[string]interface{}, error) {
	for i := range p.Variables {
		if val, err := p.Variables[i].Eval(ctx); nil == err {
			ctx[p.Variables[i].Name] = val
		} else {
			return nil, err
		}
	}
	return ctx, nil
}

func (p *Pipeline) Exec(ctx map[string]interface{}) (error, string) {
	var vars, err = p.EvalVariables(ctx)
	if nil != err {
		return err, ""
	}
	for s := range p.Steps {
		if err, str := p.Steps[s].Exec(vars); nil != err {
			return err, str
		}
	}
	return nil, ""
}
