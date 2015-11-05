package main

import (
	"fmt"
	client "lamo/tools/influxClient"
	"time"

	"github.com/qiniu/log.v1"
)

type InfluxdbTester struct {
	Conf             *Config
	StopCh, ReportCh chan struct{}
	Client           *client.InfluxClient
}

func NewInfluxdbTester(c *Config) *InfluxdbTester {

	cli := client.NewInfluxClient()
	lbcli, err := client.NewClient(c.Hosts)
	if err != nil {
		log.Error(err)
		return nil
	}

	cli.Client = lbcli
	return &InfluxdbTester{
		Conf:     c,
		StopCh:   make(chan struct{}),
		ReportCh: make(chan struct{}),
		Client:   cli,
	}
}

func (t *InfluxdbTester) Test() {

	write := func(points []byte) {
		for {
			select {
			case <-t.StopCh:
				t.Report()
				return
			case <-t.ReportCh:
				t.Report()
			}

			t.Client.WritePoints(points)
			time.Sleep(time.Duration(t.Conf.BatchInterval) * time.Millisecond)
		}
	}
	for i := 0; i < int(t.Conf.Concurrency); i++ {
		points := PointsGenerator(int(t.Conf.BatchSize))
		go write(points)
	}

}

func (t *InfluxdbTester) Report() {
	fmt.Printf("Point size:%d bytes\nPoints per Batch:%d\nBatch Size:%d bytes\nWrite per second:%d\nPoints per Second:%d\nBytes per Second:%d\n",
		1, 2, 2, 2, 2, 2)
}
