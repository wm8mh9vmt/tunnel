package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"runtime"
	"time"

	"github.com/fixiong/tunnel/tunnel"
	"gopkg.in/yaml.v2"
)

type Configure struct {
	Outside string
	Secret  string
}

var (
	module    = "inside"
	exit      chan struct{}
	loger     *log.Logger
	config    = new(Configure)
	tunnelSet = tunnel.CreateTunnelSet(createTunnel)
)

func createTunnel(address []byte) (conn io.ReadWriteCloser, err error) {
	s := string(address)
	conn, err = net.Dial("tcp", s)
	return
}

func fatal(err interface{}, where string) {
	if err != nil {
		fmt.Println(where, ": ", err)
		if loger != nil {
			loger.Fatal(where, ": ", err)

		} else {
			log.Fatal(where, ": ", err)
		}
	}
}

func check(err error) {
	if err != nil {
		_, file, line, _ := runtime.Caller(1)
		panic(fmt.Sprintln("Assert failed! ", file, " , ", line, ",", err.Error()))
	}
}

func main() {
	defer func() {
		err := recover()
		fatal(err, "main ")
	}()

	config_yml, err := ioutil.ReadFile("insideConfigure.yml")
	check(err)

	err = yaml.Unmarshal(config_yml, config)
	check(err)

	err = os.MkdirAll("log", os.ModePerm)
	check(err)

	logFileName := module + "_" + time.Now().Format("2006-01-02") + ".log"
	logFile, err := os.Create(logFileName)
	check(err)

	loger = log.New(logFile, module+"_", log.Ldate|log.Ltime|log.Lshortfile)
	fmt.Println("logging to file: " + logFileName)

	for {
		err = connection()
		if err != nil {
			fmt.Println(err)
			loger.Println(err)
		}
		time.Sleep(time.Second)
	}
}

func connection() (err error) {
	conn, err := net.Dial("tcp", config.Outside)
	if err != nil {
		return err
	}
	defer conn.Close()

	header := "tunne " + config.Secret
	_, err = conn.Write([]byte(header))
	if err != nil {
		return
	}

	err = tunnelSet.Connect(conn)
	return
}
