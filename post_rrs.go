package main

import (
	"bytes"
	"encoding/json"
	_ "fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/qiniu/log.v1"
)

func get(cmd, action, url string, dat []byte) {

	client := &http.Client{}
	log.Info(">>>>>>> "+cmd, "url", url)
	req, err := http.NewRequest(action, url, bytes.NewBuffer(dat))
	if err != nil {
		log.Error(err)
	}

	req.Header.Set("Authorization", "QiniuStub uid=1&ut=4")
	if dat != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Error(err)
	}

	defer resp.Body.Close()

	_bytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Error(err)
	}
	if string(_bytes) == "{}" {
		log.Info("ok")
	} else {
		log.Error(string(_bytes))
	}
}

func writeto(cmd, action, url string, dat []byte, method string) {

	client := &http.Client{}
	req, err := http.NewRequest(action, url, bytes.NewBuffer(dat))
	if err != nil {
		log.Error(err)
	}

	if dat != nil {
		if method == "json" {
			req.Header.Set("Content-Type", "application/json")
		} else if method == "text" {
			req.Header.Set("Content-Type", "text/plain")
		}
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Error(err)
	}
	defer resp.Body.Close()

	_bytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Error(err)
	}
	if string(_bytes) == "{}" {
		//log.Info("ok")
	} else {
		//log.Info(string(_bytes))
	}
}

//连着API gate的测试用, 不对代码进行任何封装
func Create(url string, cq bool) string {

	url = "http://" + url
	log.SetOutputLevel(0)

	client := &http.Client{}

	req, err := http.NewRequest("POST", url+"/v1/repos", nil)
	if err != nil {
		log.Error(err)
	}

	req.Header.Set("Authorization", "QiniuStub uid=1&ut=4")

	resp, err := client.Do(req)
	if err != nil {
		log.Error(err)
	}
	if resp.StatusCode != 200 {
		log.Error("create repo fail", resp.StatusCode)
		dat, _ := ioutil.ReadAll(resp.Body)
		log.Debug(string(dat))
	}

	defer resp.Body.Close()

	_bytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Error(err)
	}

	var repo map[string]string
	json.Unmarshal(_bytes, &repo)

	log.Debug("create repo:", repo)

	//_______________________________________________

	retentionParams := []byte(`{"name":"qiniu_evm","duration":"2h"}`)

	get("create retention: qiniu_evm", "POST", url+"/v1/repos/"+repo["id"]+"/retentions/qiniu_evm", retentionParams)

	//________________________________________________

	seriesParams := []byte(`{"retention":"qiniu_evm"}`)

	get("create series: qiniu_evm.qiniu_test", "POST", url+"/v1/repos/"+repo["id"]+"/series/qiniu_test", seriesParams)

	//________________________________________________

	seriesParamsCpu := []byte(`{"retention":"qiniu_evm"}`)

	get("create series: qiniu_evm.cpu", "POST", url+"/v1/repos/"+repo["id"]+"/series/cpu", seriesParamsCpu)

	//_________

	seriesParamsMem := []byte(`{"retention":"qiniu_evm"}`)

	get("create series: qiniu_evm.mem", "POST", url+"/v1/repos/"+repo["id"]+"/series/mem", seriesParamsMem)

	//______________

	seriesParamsDisk := []byte(`{"retention":"qiniu_evm"}`)

	get("create series: qiniu_evm.disk", "POST", url+"/v1/repos/"+repo["id"]+"/series/disk", seriesParamsDisk)

	if cq {
		createCq(url, repo["id"], 2)
	}
	return repo["id"]

}

func createCq(url, repoid string, n int64) {

	time.Sleep(time.Duration(n) * time.Second)

	//url = "http://" + url
	//cpu
	createCqParams := []byte(`{
			"retention" : "qiniu_evm",
			"sql": "SELECT mean(value) as value INTO cpu_2m_mean FROM cpu GROUP BY time(2m), region, host"
		}`)

	get("create cq: cpu_2m_mean", "POST", url+"/v1/repos/"+repoid+"/views/cpu_2m_mean", createCqParams)
	time.Sleep(time.Duration(n) * time.Second)

	createCqParams = []byte(`{
			"retention" : "qiniu_evm",
			"sql": "SELECT count(value) as value INTO cpu_2m_count FROM cpu  GROUP BY time(2m), region, host"
		}`)

	get("create cq: cpu_2m_count", "POST", url+"/v1/repos/"+repoid+"/views/cpu_2m_count", createCqParams)
	time.Sleep(time.Duration(n) * time.Second)

	createCqParams = []byte(`{
			"retention" : "qiniu_evm",
			"sql": "SELECT sum(value) as value INTO cpu_2m_sum FROM cpu GROUP BY time(2m), region"
		}`)

	get("create cq: cpu_2m_count", "POST", url+"/v1/repos/"+repoid+"/views/cpu_2m_sum", createCqParams)

	/*//mem
	createCqParams = []byte(`{
			"retention" : "qiniu_evm",
			"sql": "SELECT mean(value) as value INTO mem_2m_count FROM mem where time < now() GROUP BY time(2m), region"
		}`)

	get("create cq: mem_2m_count", "POST", url+"/v1/repos/"+repoid+"/views/mem_2m_count", createCqParams)

	createCqParams = []byte(`{
			"retention" : "qiniu_evm",
			"sql": "SELECT count(value) as value INTO mem_2m_mean FROM mem  where time < now() GROUP BY time(2m), region"
		}`)

	get("create cq: mem_2m_mean", "POST", url+"/v1/repos/"+repoid+"/views/mem_2m_mean", createCqParams)

	//disk
	createCqParams = []byte(`{
			"retention" : "qiniu_evm",
			"sql": "SELECT mean(value) as value INTO disk_2m_mean FROM disk where time < now() GROUP BY time(2m), region"
		}`)

	get("create cq: disk_2m_mean", "POST", url+"/v1/repos/"+repoid+"/views/disk_2m_mean", createCqParams)

	createCqParams = []byte(`{
			"retention" : "qiniu_evm",
			"sql": "SELECT count(value) as value INTO disk_2m_count FROM disk where time < now() GROUP BY time(2m), region"
		}`)

	get("create cq: disk_2m_count", "POST", url+"/v1/repos/"+repoid+"/views/disk_2m_count", createCqParams)
	*/
}
