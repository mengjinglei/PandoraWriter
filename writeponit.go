package main

import (
	"bytes"
	"encoding/json"
	_ "fmt"
	"github.com/qiniu/log.v1"
	_ "github.com/rakyll/ticktock"
	_ "github.com/rakyll/ticktock/t"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"time"
)

func init() {
	log.SetOutputLevel(0)
}

type InfluxJob struct {
	repoid string
	repoN  int
	debug  bool
	cq     bool
	pointN int
	client *http.Client
	url    string
}

type tag struct {
	Region string `json:"region"`
	Host   string `json:"host"`
}
type field struct {
	Value       float32 `json:"value"`
	Temperature float32 `json:"temperature"`
}

type Point struct {
	Tags   tag   `json:"tags"`
	Fields field `json:"fields"`
}

var (
	r1      = rand.New(rand.NewSource(time.Now().UnixNano()))
	r2      = rand.New(rand.NewSource(time.Now().UnixNano()))
	regions = [4]string{"BJ", "SH", "HZ", "NJ"}
	hosts   = [4]string{"server01", "server02", "server03", "server04"}
	series  = [3]string{"cpu", "mem", "disk"}
)

func (job *InfluxJob) Run() (err error) {

	/*	pp := []Point{
			Point{
				tag{
					Host:   hosts[r1.Intn(4)],
					Region: regions[r1.Intn(4)],
				},
				field{
					Value: r2.Float32(),
				},
			},
			Point{
				tag{
					Host:   hosts[r2.Intn(4)],
					Region: regions[r2.Intn(4)],
				},
				field{
					Value: r2.Float32(),
				},
			},
		}
	*/
	p := make([]Point, 0)

	for i := 0; i < job.pointN; i++ {
		pp := Point{
			tag{
				Host:   hosts[r1.Intn(4)],
				Region: regions[r1.Intn(4)],
			},
			field{
				Value: r2.Float32(),
			},
		}
		p = append(p, pp)
	}

	buf, err := json.Marshal(p)
	if err != nil {
		return
	}

	//writeto("--", "POST", job.url+"/v1/repos/"+job.repoid+"/series/"+series[r1.Intn(3)]+"/points", buf)
	if job.debug {
		log.Debug(job.url+"/v1/repos/"+job.repoid+"/series/cpu/points", string(buf))

	}
	req1, err := http.NewRequest("POST", job.url+"/v1/repos/"+job.repoid+"/series/cpu/points", bytes.NewBuffer(buf))
	if err != nil {
		log.Error(err)
	}

	req1.Header.Set("Authorization", "QiniuStub uid=1&ut=4")
	req1.Header.Set("Content-Type", "application/json")
	resp1, err := job.client.Do(req1)
	if err != nil {
		log.Error(err)
	}
	if job.debug {
		ret, eerr := ioutil.ReadAll(resp1.Body)
		if eerr != nil {
			return
		}
		log.Info(string(ret))

	}

	defer resp1.Body.Close()
	return

}

func Write(job InfluxJob, url, drt string, n int64) {

	var count, step int64
	step = 1000

	//go createCq(url, job.repoid, n)
	count = 1
	log.Debug(time.Now().String())

	for {
		count = count + 1
		if count%step == 0 {
			log.Debug("points count:", count, time.Now().String())
			// total = 0
		}
		if count == step/10 && job.cq {
			createCq(url, job.repoid, n)
		}
		job.Run()

		time.Sleep(time.Duration(n) * time.Millisecond)
	}

}

type influxPoint struct {
	Measurement string `json:"measurement"`
	Tags        tag    `json:"tags"`
	Fields      field  `json:"fields"`
}

type influxWrite struct {
	Database  string        `json:"database"`
	Retention string        `json:"retentionPolicy"`
	Points    []influxPoint `json:"points"`
}

func WriteDefault(method string, n int64) (err error) {

	resp, err := http.Get("http://127.0.0.1:8086/query?q=DROP+DATABASE+testDB")
	resp, err = http.Get("http://127.0.0.1:8086/query?q=CREATE+DATABASE+testDB")
	if err != nil {
		return err
	}
	log.Debug(resp)

	database := "testDB"

	go func() {
		time.Sleep(time.Duration(n*100) * time.Millisecond)
		log.Debug("create cq cpu_2m_count")
		resp, err := http.Get("http://127.0.0.1:8086/query?q=" + url.QueryEscape("create continuous query cpu_2m_count on testDB begin select count(value) into cpu_2m_count from cpu where time < now() group by time(2m) end"))
		if err != nil {
			return
		}
		log.Debug(resp)

		log.Debug("create cq mem_2m_mean")
		resp, err = http.Get("http://127.0.0.1:8086/query?q=" + url.QueryEscape("create continuous query mem_2m_mean on testDB begin select count(value) into mem_2m_mean from mem where time < now() group by time(2m) end"))
		if err != nil {
			return
		}
		log.Debug(resp)

		log.Debug("create cq disk_3m_max")
		resp, err = http.Get("http://127.0.0.1:8086/query?q=" + url.QueryEscape("create continuous query disk_3m_max on testDB begin select count(value) into disk_3m_max from disk where time < now() group by time(3m) end"))
		if err != nil {
			return
		}
		log.Debug(resp)

	}()

	if method == "json" {
		retention := "default"
		for {
			pp := influxWrite{
				database,
				retention,
				[]influxPoint{
					influxPoint{
						series[r1.Intn(3)],
						tag{
							Host:   hosts[r1.Intn(4)],
							Region: regions[r1.Intn(4)],
						},
						field{
							Value:       r2.Float32() * 5,
							Temperature: r2.Float32() * 40,
						},
					},
					influxPoint{
						series[r1.Intn(3)],
						tag{
							Host:   hosts[r1.Intn(4)],
							Region: regions[r1.Intn(4)],
						},
						field{
							Value:       r2.Float32() * 10,
							Temperature: r2.Float32() * 40,
						},
					},
				},
			}
			dat, err := json.Marshal(pp)
			if err != nil {
				return err
			}
			log.Debug(string(dat))
			writeto("--", "POST", "http://127.0.0.1:8086/write", dat, "json")

			time.Sleep(time.Duration(n) * time.Millisecond)
		}
	} else if method == "text" {
		for {
			dat := []byte(`cpu,host=` + hosts[r1.Intn(4)] + `,region=` + regions[r1.Intn(4)] + ` value=0.64,temperature=37.6`)
			writeto("--", "POST", "http://127.0.0.1:8086/write?db="+database+"&rp=default", dat, method)

			dat = []byte(`disk,host=` + hosts[r1.Intn(4)] + `,region=` + regions[r1.Intn(4)] + ` value=0.64,temperature=37.6`)
			writeto("--", "POST", "http://127.0.0.1:8086/write?db="+database+"&rp=default", dat, method)

			dat = []byte(`mem,host=` + hosts[r1.Intn(4)] + `,region=` + regions[r1.Intn(4)] + ` value=0.64,temperature=37.6`)
			writeto("--", "POST", "http://127.0.0.1:8086/write?db="+database+"&rp=default", dat, method)

			time.Sleep(time.Duration(n) * time.Millisecond)
		}

	}

	return

}
