package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"
	"time"
	"unsafe"

	_ "unsafe"

	"github.com/beanstalkd/go-beanstalk"
	"github.com/go-resty/resty/v2"
)

type Configure struct {
	Host    string
	Port    uint
	Tube    string
	Command string
}

func (c *Configure) bnsServerDns() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

var bnsConfig Configure

func Byte2String(bytes []byte) string {
	str := (*string)(unsafe.Pointer(&bytes))
	return *str
}

func CatchError(skip int, err error) {
	if err == nil {
		return
	}
	pc, file, line, ok := runtime.Caller(skip)
	errorMessage := err.Error()
	if ok {
		//获取函数名
		pcName := runtime.FuncForPC(pc).Name()
		errorMessage = fmt.Sprintf("%s   %d   %s   %s", file, line, pcName, errorMessage)
	}
	fmt.Println(errorMessage)
}

// 连接beanstalk
func ConnectBeanstalk() (c *beanstalk.Conn) {
	c, err := beanstalk.Dial("tcp", bnsConfig.bnsServerDns())
	if err != nil {
		panic(err)
	}
	return
}

// 状态监控
func Status() {
	bns := ConnectBeanstalk()
	defer bns.Close()

	// global tube
	// status, err := bns.Stats()

	// single tube
	tube := &beanstalk.Tube{Conn: bns, Name: bnsConfig.Tube}
	m, err := tube.Stats()
	if err != nil {
		log.Fatal(err)
	}

	strtpl := `Tube name: %s, watching consumers: %s, jobs ready/total: %s/%s`
	outString := fmt.Sprintf(
		strtpl,
		m["name"],
		m["current-watching"],
		m["current-jobs-ready"],
		m["total-jobs"],
	)
	fmt.Println(outString)
}

// 生产端
func Producer() {
	bns := ConnectBeanstalk()
	defer bns.Close()

	content := "http://local.com/api/address"
	tube := &beanstalk.Tube{Conn: bns, Name: bnsConfig.Tube}
	for i := 0; i < 1000; i++ {
		id, err := tube.Put([]byte(content), 1, 0, 120*time.Second)
		CatchError(1, err)
		fmt.Println(fmt.Sprintf("job id: %d put in.", id))
	}
}

// 消费端
func Consumer() {
	chanPools := make(chan uint, 50)
	bns := ConnectBeanstalk()
	defer bns.Close()

	tubeSet := beanstalk.NewTubeSet(bns, bnsConfig.Tube)
	for {
		id, body, err := tubeSet.Reserve(5 * time.Second)
		if err != nil {
			if strings.HasSuffix(err.Error(), "timeout") {
				continue
			}
			CatchError(1, err)
		}
		content := Byte2String(body)
		if content == "" || !strings.HasPrefix(content, "http") {
			bns.Bury(id, 99)
			continue
		}
		chanPools <- 1
		go func(link string) {
			client := resty.New()
			resp, err := client.R().Get(link)
			CatchError(1, err)
			message := fmt.Sprintf("%d %s - %s - %s",
				resp.StatusCode(), resp.Time(), link, Byte2String(resp.Body()))
			log.Println(message)
			<-chanPools
		}(content)
		bns.Delete(id)
	}
}

func Usage() {
	fmt.Fprintf(os.Stderr, `Consumer client for beanstalkd, version: 1.0.0
Usage: ./bns [-h] [-c command] [-l host] [-p port] [-t tube]

Options:
`)
	flag.PrintDefaults()
	fmt.Fprintf(os.Stderr, `
Examples:
  help: ./bns -h
  state: ./bns -c status -l 127.0.0.1 -p 11300 -t default
  consume: ./bns -c consume -l 127.0.0.1 -p 11300 -t default

`)
}

func main() {
	showHelp := false
	flag.Usage = Usage
	flag.BoolVar(&showHelp, "h", false, "this help")
	flag.StringVar(&bnsConfig.Host, "l", "127.0.0.1", "server address")
	flag.UintVar(&bnsConfig.Port, "p", 11300, "server port")
	flag.StringVar(&bnsConfig.Tube, "t", "", "tube name")
	flag.StringVar(&bnsConfig.Command, "c", "status", "command type[status|consume]")
	flag.Parse()

	if !showHelp {
		if len(os.Args) == 1 {
			showHelp = true
		} else {
			showHelp = os.Args[1] == "help" || os.Args[1] == "-h"
		}
	}

	if showHelp {
		flag.Usage()
		return
	}

	if bnsConfig.Tube == "" {
		fmt.Println("You must specify the tube name by -t (call help with parameter: -h)")
		return
	}

	switch bnsConfig.Command {
	case "status":
		Status()
	case "produce":
		Producer()
	case "consume":
		Consumer()
	default:
		flag.Usage()
	}
}
