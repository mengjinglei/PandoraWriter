package main

import (
	"bytes"
	"encoding/json"
	_ "fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"time"

	"github.com/qiniu/http/httputil.v1"

	"runtime/pprof"

	"os"

	"github.com/influxdb/influxdb/client"
	"github.com/qiniu/log.v1"
)

func init() {
	log.SetOutputLevel(0)
}

type InfluxJob struct {
	repoid   string
	repoN    int
	debug    bool
	cq       bool
	method   string
	pointN   int
	client   *http.Client
	url      string
	threadn  int
	interval int64

	points    int64
	start     time.Time
	totalLast int64
	pointSize int64
	con       *client.Client
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

	//job.start = time.Now()
	var step int64
	step = 1000
	var dat bytes.Buffer
	f, err := os.Create("heap.prof")
	if err != nil {
		log.Fatal(err)
	}
	pprof.WriteHeapProfile(f)
	p := make([]*Point, 0)
	for {
		job.points += 1
		if job.points%step == 0 {
			last := int64(time.Now().Sub(job.start) / time.Millisecond)
			if last < 1 {
				last = 1
			}
			job.totalLast += last
			rate := (step * 1000) / last
			avgRate := (job.points * 1000) / job.totalLast
			log.Debug("point size", job.pointSize, " insert total", job.points, "last ", last, "rate:", rate, job.pointSize*rate/1024, "avg rate:", avgRate, job.pointSize*avgRate/1024)
			job.start = time.Now()
		}
		if job.points == 100 && job.cq {
			go createCq(job.url, job.repoid, 10)
		}

		if job.method == "json" {
			//write json

			p = p[:0]
			for i := 0; i < job.pointN; i++ {
				pp := &Point{
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

			buf, MarshalErr := json.Marshal(p)
			if MarshalErr != nil {
				return
			}
			dat.WriteString(string(buf))

		} else if job.method == "text" {
			var pts string
			for i := 0; i < job.pointN; i++ {
				pt := "host=" + hosts[r1.Intn(4)] + ",region=" + regions[r1.Intn(4)] + " value=0.64,temperature=37.6\n"
				pts += pt
			}
			dat.WriteString(string(pts))
		}

		// write plain text

		if job.debug {
			log.Debug(job.url+"/v1/repos/"+job.repoid+"/series/cpu/points", dat.String())

		}
		job.pointSize = int64(dat.Len())
		req1, err := http.NewRequest("POST", job.url+"/v1/repos/"+job.repoid+"/series/cpu/points", &dat)
		if err != nil {
			log.Error(err)
		}

		req1.Header.Set("Authorization", "QiniuStub uid=1&ut=4")
		if job.method == "json" {
			req1.Header.Set("Content-Type", "application/json")
		} else if job.method == "text" {
			req1.Header.Set("Content-Type", "application/text")
		}

		resp1, err := job.client.Do(req1)
		if err != nil {
			log.Error(err)
		}

		if resp1.StatusCode != 200 {
			err := httputil.NewError(600, "write data point fail, status code is "+string(resp1.StatusCode))
			log.Debug(err)
			log.Debug(resp1.StatusCode, resp1.Status)
			return err
		}

		if job.debug {
			ret, eerr := ioutil.ReadAll(resp1.Body)
			if eerr != nil {
				return eerr
			}

			log.Info(string(ret))
		}

		defer resp1.Body.Close()
		io.Copy(ioutil.Discard, resp1.Body)

		time.Sleep(time.Duration(job.interval) * time.Millisecond)

	}
	return

}

func Write(job InfluxJob, url, drt string, n int64) {

	//	var count, step int64
	//	step = 1000

	//go createCq(url, job.repoid, n)
	job.points = 1
	job.start = time.Now()
	job.totalLast = 0

	for i := 0; i < job.threadn; i++ {
		job.Run()
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
		time.Sleep(time.Duration(100) * time.Second)
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
		count := 0
		log.Debug("start: ", time.Now().String())
		for {
			count += 1
			dat := []byte(`cpu,host=` + hosts[r1.Intn(4)] + `,region=` + regions[r1.Intn(4)] + ` value=0.64,temperature=37.6`)
			writeto("--", "POST", "http://127.0.0.1:8086/write?db="+database+"&rp=default", dat, method)

			if count%1000 == 0 {
				log.Debug("end: ", time.Now().String())
			}
			time.Sleep(time.Duration(n) * time.Millisecond)
		}

	}

	return

}
