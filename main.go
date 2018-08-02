package main

import (
	"flag"
	"io/ioutil"
	"math/rand"
	"time"

	yaml "gopkg.in/yaml.v2"
)

var (
	KeyPrefix string
	Cfg       *Config
	Cs        string
)

type Config struct {
	Common struct {
		Thread int
		KeyNum int    `yaml:"key_num"`
		KeyFmt string `yaml:"key_fmt"`
		ValFmt string `yaml:"val_fmt"`
	}
	Aerospike struct {
		Host      []string
		Namespace string
		Set       string
		Bin       string
	}
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func main() {
	var (
		file string
		typ  string
		gen  bool
		pre  string
	)
	flag.StringVar(&file, "c", "bt.yml", "config file")
	flag.StringVar(&typ, "t", "aerospike", "bench type")
	flag.StringVar(&pre, "p", "benchmark", "key prefix")
	flag.BoolVar(&gen, "g", false, "create testing data")
	flag.Parse()

	b, err := ioutil.ReadFile(file)
	if err != nil {
		panic(err)
	}
	if err = yaml.Unmarshal(b, &Cfg); err != nil {
		panic(err)
	}

	KeyPrefix = pre + Cfg.Common.KeyFmt

	bm, err := NewBenchmarkTool(typ)
	if err != nil {
		panic(err)
	}
	if gen {
		bm.GenData()
	} else {
		bm.Start()
	}
}

type BenchmarkTool interface {
	Start()
	GenData()
}

func NewBenchmarkTool(typ string) (BenchmarkTool, error) {
	switch typ {
	case "aerospike":
		return NewBenchmarkAerospike()
	default:
		panic("unsupport bench type")
	}
	return nil, nil
}
