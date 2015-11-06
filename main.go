package main

import (
	_ "fmt"
	"net/http"
	"time"

	"github.com/qiniu/log.v1"
	"github.com/qiniu/rpc.v3"
	"github.com/qiniu/rpc.v3/lb"
	"qbox.us/cc/config"
	"qiniu.com/auth/authstub.v1"
	"qiniu.com/auth/proto.v1"
)

type Config struct {
	Target        string   `json:"target"` //pandora or influxdb
	Hosts         []string `json:"hosts"`
	BatchSize     uint     `json:"batchSize"`
	BatchInterval uint     `json:"batchInterval"` //time duration between two batch write, unit:ms
	TestTime      int64    `json:"testTime"`      // how long the test last, unit: minute
	SeriesN       uint     `json:"seriesN"`       // how many series to concurrency write to
	Concurrency   uint     `json:"concurrency"`   // the number of simultaneous writes to run
}

type Tester struct {
	Target string

	Client interface {
		Test()
	}
}

func NewTester(target string) *Tester {

	return &Tester{
		Target: target,
	}
}

func (t *Tester) Test() {
	t.Client.Test()
}

func main() {
	config.Init("f", "throughput-test", "defaul.conf")
	var conf Config
	if err := config.Load(&conf); err != nil {
		log.Fatal("load config file fail")
		return
	}

	tester := NewTester(conf.Target)
	switch conf.Target {
	case "influxdb":
		tester.Client = NewInfluxdbTester(&conf)
	case "pandora":
		log.Info("not implement yet!")
		return
	}

	tester.Test()

}

func newLbClient(hosts []string) (lbclient *lb.Client, err error) {

	var t http.RoundTripper
	tc := &rpc.TransportConfig{
		DialTimeout:           time.Second * 10,
		ResponseHeaderTimeout: time.Second * 10,
	}
	t = rpc.NewTransport(tc)

	si := &proto.SudoerInfo{
		UserInfo: proto.UserInfo{
			Uid:   1,
			Utype: 4,
		},
	}
	t = authstub.NewTransport(si, t)

	lbConfig := &lb.Config{
		Http:              &http.Client{Transport: t},
		FailRetryInterval: 0,
		TryTimes:          1,
	}

	lbclient, err = lb.New(hosts, lbConfig)
	if err != nil {
		return nil, err
	}

	return
}
