package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/qiniu/log.v1"
)

type HttpTester struct {
	Conf *Config
}

func NewHttpTester(conf *Config) *HttpTester {

	return &HttpTester{
		Conf: conf,
	}
}

func (s *HttpTester) Test() {

	client := &http.Client{}
	write := func() {
		for {
			points := PointsGenerator(int(s.Conf.BatchSize))
			fmt.Printf(".")
			req, err := http.NewRequest("POST", s.Conf.Hosts[0], bytes.NewReader(points))
			if err != nil {
				log.Println(err)
			}
			resp, err := client.Do(req)
			if err != nil {
				log.Error(err)
			}
			if resp != nil {
				io.Copy(ioutil.Discard, resp.Body)
				resp.Body.Close()
			}

			time.Sleep(time.Duration(s.Conf.BatchInterval) * time.Millisecond)
		}
	}

	for i := 0; i < int(s.Conf.Concurrency); i++ {
		go write()
	}

	select {
	case <-time.After(time.Duration(s.Conf.TestTime) * time.Minute):
		//t.Wg.Wait()
		//s.Report()
	}
}
