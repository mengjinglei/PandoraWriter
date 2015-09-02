package main

import (
	"flag"
	_ "fmt"
	"net/http"
	"time"

	"qiniu.com/auth/authstub.v1"
	"qiniu.com/auth/proto.v1"

	"github.com/qiniu/log.v1"
	"github.com/qiniu/rpc.v3"
	"github.com/qiniu/rpc.v3/lb"
)

func main() {
	f := flag.String("f", "all", "specify which method to run:\n\t<all>: <create> + <write>\n\t<create>:create repoid,series and rp\n\t<write>: write data point\n\t<test>: =<all> operate direct on influxdb:8086")
	URL := flag.String("url", "http://127.0.0.1:8899", "url when create metadata and write data point")
	repo := flag.String("repo", "", "repoid")
	repoN := flag.Int("repon", 1, "the number of repo to write data point to")
	pointN := flag.Int("pointn", 1, "the number of points with one write operation")
	interval := flag.Int64("n", 500, "ms")
	minute := flag.Int64("minute", 16, "minute to run the test")
	debug := flag.Bool("d", false, "debug, default false")
	method := flag.String("method", "text", "write data points in application/json format or text/plain")
	Cq := flag.Bool("cq", true, "specify whether create cq during writing data points, default false")
	threadn := flag.Int("threadn", 1, "specify how many threads write data cocurrently")
	Series := flag.String("series", "", "specify which series to write data in")

	flag.Parse()

	done := make(chan bool, 1)

	if *f == "" {
		log.Fatal("You have to specify the method to run!")
		return
	}

	log.Printf("cmd:%s,url:%s,repo:%s,repoN:%d,interval:%d ms, method:%s", *f, *URL, *repo, *repoN, *interval, *method)

	tr := &http.Transport{
		DisableCompression: true,
		DisableKeepAlives:  true,
	}
	client := &http.Client{Transport: tr}

	if *f == "curl" {
		client, err := newLbClient([]string{*URL})
		if err != nil {
			log.Debug(err)
			return
		}
		Curl(client, *interval)
		return
	} else if *f == "test" {

		WriteInfluxdb(*method, *interval)
		return

	} else if *f == "create" {

		for i := 0; i < *repoN; i++ {
			Create(*URL, *Cq)
		}
		return

	} else if *f == "write" {
		//client := &http.Client{}
		if *Series == "" {
			log.Fatal("series must specified")
		}

		job := InfluxJob{repoid: *repo, series: *Series, threadn: *threadn, interval: *interval, debug: *debug, method: *method, client: client, repoN: *repoN, pointN: *pointN, url: "http://" + *URL}

		log.Info("start write repo:", *repo)
		if *repo == "" {
			log.Error("repo cannot be null!")
		} else {
			Write(job, *URL, "", *interval)
		}
		time.Sleep(time.Duration(*minute) * time.Minute)

		return

	} else if *f == "all" {

		for i := 0; i < *repoN; i++ {
			log.Info("start create")
			repoid := Create(*URL, *Cq)
			go func() {
				log.Info("create repo", repoid, "complete")
				log.Info("start to write", repoid, "to", *URL, "interval", *interval, "ms")
				//client := &http.Client{}

				job := InfluxJob{repoid: repoid, cq: *Cq, threadn: *threadn, interval: *interval, method: *method, debug: *debug, client: client, repoN: *repoN, pointN: *pointN, url: "http://" + *URL}

				Write(job, *URL, "", *interval)
			}()
			time.Sleep(time.Duration(2) * time.Second)

		}

		//time.Sleep(time.Duration(*minute) * time.Minute)
		<-done
	}

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
