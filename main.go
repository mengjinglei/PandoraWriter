package main

import (
	"flag"
	"fmt"
	"github.com/qiniu/log.v1"
)

func main() {
	f := flag.String("f", "all", "specify which method to run:\n<all>: <create> + <write>\n<create>:create repoid,series and rp\n<write>: write data point\n<test>: =<all> operate direct on influxdb:8086")
	URL := flag.String("url", "127.0.0.1:7777", "url when create metadata")
	repo := flag.String("repo", "", "repoid")
	interval := flag.Int64("n", 100, "ms")
	method := flag.String("method", "text", "write data points in application/json format or text/plain")

	flag.Parse()

	if *f == "" {
		log.Fatal("You have to specify the method to run!")
		return
	}

	if *f == "test" {

		WriteDefault(*method, *interval)
		return

	} else if *f == "create" {

		Create(*URL)
		return

	} else if *f == "write" {

		fmt.Println(">>>>>>>>>>>start write repo:", *repo)
		if *repo == "" {
			log.Error("repo cannot be null!")
		} else {
			Write(*repo, *URL, "", *interval)

		}
		return

	} else if *f == "all" {

		fmt.Println(">>>>>>>>>>>start create")
		repoid := Create(*URL)
		fmt.Println("create repo", repoid, "complete")
		fmt.Println("start to write", repoid, "to", *URL, "interval", *interval, "ms")
		Write(repoid, *URL, "", *interval)
		return

	}

}
