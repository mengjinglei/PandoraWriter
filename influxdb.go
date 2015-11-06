package main

import (
	"fmt"
	client "lamo/tools/influxClient"
	"sync"
	"time"

	"github.com/qiniu/log.v1"
)

type InfluxdbTester struct {
	WriteN           uint64
	FailN            int64
	Psize            uint64
	Conf             *Config
	StopCh, ReportCh chan struct{}
	CountCh          chan struct{}
	FailCh           chan struct{}
	Client           *client.InfluxClient
	Wg               sync.WaitGroup
}

func NewInfluxdbTester(c *Config) *InfluxdbTester {

	cli := client.NewInfluxClient()
	lbcli, err := client.NewClient(c.Hosts)
	if err != nil {
		log.Error(err)
		return nil
	}

	cli.Client = lbcli
	var wg sync.WaitGroup
	return &InfluxdbTester{
		WriteN:   0,
		Psize:    0,
		Conf:     c,
		StopCh:   make(chan struct{}),
		ReportCh: make(chan struct{}),
		CountCh:  make(chan struct{}),
		FailCh:   make(chan struct{}),
		Client:   cli,
		Wg:       wg,
	}
}

func (t *InfluxdbTester) Test() {
	//ensure database
	_, err := t.Client.Query("drop database testDB")
	if err != nil {
		log.Error(err)
	}
	_, err = t.Client.Query("create database testDB")
	if err != nil {
		log.Error(err)
		return
	}

	_, err = t.Client.Query("alter retention policy default on testDB duration 100d replication 1 ")
	if err != nil {
		log.Error(err)
		return
	}

	go func() {
		for {
			select {
			case <-t.CountCh:
				t.WriteN++
			case <-t.StopCh:
				break
			case <-t.FailCh:
				t.FailN++
			}
		}
	}()

	write := func(points []byte) {
		for {

			select {
			case <-t.StopCh:
				t.Wg.Done()
				return
			default:
			}

			err := t.Client.WritePoints(points)
			if err != nil {
				log.Error(err)
				t.FailCh <- struct{}{}
				continue
			}
			t.CountCh <- struct{}{}
			time.Sleep(time.Duration(t.Conf.BatchInterval) * time.Millisecond)
		}
	}

	for i := 0; i < int(t.Conf.Concurrency); i++ {
		t.Wg.Add(1)
		points := PointsGenerator(int(t.Conf.BatchSize))
		t.Psize = uint64(len(points))
		go write(points)
	}

	fmt.Printf("Point size:%d bytes\nPoints per Batch:%d\nBatch Size:%d bytes\n",
		t.Psize/uint64(t.Conf.BatchSize), t.Conf.BatchSize, t.Psize)

	select {
	case <-time.After(time.Duration(t.Conf.TestTime) * time.Minute):
		for i := 0; i <= int(t.Conf.Concurrency); i++ {
			t.StopCh <- struct{}{}
		}
		//t.Wg.Wait()
		t.Report()
	}

}

func (t *InfluxdbTester) Report() {
	t.Psize = t.Psize
	wps := int64(t.WriteN) / 60 / t.Conf.TestTime
	fmt.Printf("Write per second:%d\nPoints per Second:%d\nBytes per Second:%d\n",
		wps, wps*int64(t.Conf.BatchSize), wps*int64(t.Psize))
	fmt.Printf("write fail count:%d\n", t.FailN)
	resp, err := t.Client.Query("select count(value) from cpu")
	if err != nil {
		log.Error(err)
		return
	} else {
		fmt.Println(resp["results"])
	}

}
