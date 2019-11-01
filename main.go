package main

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"time"
	"unsafe"

	"encoding/json"
	_ "unsafe"

	"github.com/beanstalkd/go-beanstalk"
)

type Configure struct {
	Host string
	Port int
}

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
	log.Fatal(errorMessage)
}

// 连接beanstalk
func ConnectBeanstalk() (c *beanstalk.Conn, err error) {
	var config = Configure{
		Host: "127.0.0.1",
		Port: 11300,
	}
	addr := fmt.Sprintf("%s:%d", config.Host, config.Port)
	c, err = beanstalk.Dial("tcp", addr)
	if err != nil {
		panic(err)
	}
	return
}

// 生产端
func Producer() {
	c, _ := ConnectBeanstalk()
	defer c.Close()

	// put to tube: default
	id, err := c.Put([]byte("hello"), 1, 0, 120*time.Second)
	CatchError(1, err)
	fmt.Println(id)

	// put to tube: iepngs
	tube := &beanstalk.Tube{Conn: c, Name: "iepngs"}
	id, err = tube.Put([]byte("hello"), 1, 0, 120*time.Second)
	CatchError(1, err)
	fmt.Println(id)

	fmt.Println(c.ListTubes())
}

// 消费端
func Consumer() {
	c, _ := ConnectBeanstalk()
	defer c.Close()

	fmt.Println(c.ListTubes())

	// reserve from tube: default
	id, body, err := c.Reserve(5 * time.Second)
	CatchError(1, err)
	fmt.Println(id)
	fmt.Println(Byte2String(body))
	c.Delete(id)

	// reserve from tube: iepngs
	tubeSet := beanstalk.NewTubeSet(c, "iepngs")
	// set multi:
	// tubeSet := beanstalk.NewTubeSet(c, "bns1", "bns2")
	id, body, err = tubeSet.Reserve(5 * time.Second)
	CatchError(1, err)
	fmt.Println(id)
	fmt.Println(Byte2String(body))
	c.Delete(id)
}

// 状态监控
func Status() {
	c, _ := ConnectBeanstalk()
	fmt.Println(c.ListTubes())
	status, err := c.Stats()
	if err != nil {
		log.Fatal(err)
	}
	str, err := json.Marshal(status)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(str))
	c.Close()
}

func main() {
	if len(os.Args) == 1 {
		log.Println("Usage: go run main.go consume|produce|status [default: consume]")
		os.Exit(0)
	}

	switch os.Args[1] {
	case "status":
		Status()
	case "produce":
		Producer()
	default:
		Consumer()
	}
}
