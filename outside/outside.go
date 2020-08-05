package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"runtime"
	"time"

	tunnel "github.com/fixiong/tunnel/tunnel"
	"gopkg.in/yaml.v2"
)

type TunnelConf struct {
	Port    string
	Address string
}

type Configure struct {
	Mapping []TunnelConf
	Secret  string
	Port    string
}

var (
	module    = "outside"
	exit      chan struct{}
	loger     *log.Logger
	config    = new(Configure)
	rnd       = rand.New(rand.NewSource(time.Now().UnixNano()))
	tunnelSet *tunnel.TunnelSet
	session   uint64
)

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

	config_yml, err := ioutil.ReadFile("outsideConfigure.yml")
	check(err)

	err = yaml.Unmarshal(config_yml, config)
	check(err)

	err = os.MkdirAll("log", os.ModePerm)
	check(err)

	logFileName := "./log/" + module + "_" + time.Now().Format("2006-01-02") + ".log"
	logFile, err := os.Create(logFileName)
	check(err)

	loger = log.New(logFile, module+"_", log.Ldate|log.Ltime|log.Lshortfile)
	fmt.Println("logging to file: " + logFileName)

	privateServer()

	for _, mapping := range config.Mapping {
		publicServer(mapping.Port, mapping.Address)
	}

	<-exit
	loger.Println("exit!")
	fmt.Println("exit!")

}

func privateServer() {
	listener, err := net.Listen("tcp", ":"+config.Port)
	check(err)
	header := "tunne " + config.Secret
	closeHeader := "close " + config.Secret
	start_signal := "start"
	go func() {
		//defer func() {
		//	err := recover()
		//	fatal(err, "privateServer ")
		//}()

		for {
			err := func() (err error) {
				conn, err := listener.Accept()
				if err != nil {
					return
				}
				defer conn.Close()

				header_buf := make([]byte, len(header))
				_, err = io.ReadFull(conn, header_buf)
				if err != nil {
					return
				}
				if closeHeader == string(header_buf) {
					exit <- struct{}{}
					return
				}
				if header != string(header_buf) {
					err = errors.New("wrong header!")
					return
				}

				var remote_session uint64
				err = binary.Read(conn, binary.LittleEndian, &remote_session)
				if err != nil {
					return
				}

				if tunnelSet == nil || session != remote_session {
					if tunnelSet != nil {
						tunnelSet.Close()
					}
					tunnelSet = tunnel.CreateTunnelSet(nil)
					session = rnd.Uint64()
					for session == remote_session || session == 0 {
						session = rnd.Uint64()
					}
				}

				err = binary.Write(conn, binary.LittleEndian, session)
				if err != nil {
					return
				}

				start_buf := make([]byte, len(start_signal))
				_, err = io.ReadFull(conn, start_buf)
				if err != nil {
					return
				}

				if start_signal != string(start_buf) {
					err = errors.New("wrong start signal!")
					return
				}

				fmt.Println("connection established!")
				err = tunnelSet.Connect(conn)
				return
			}()
			if err != nil {
				fmt.Println(err)
				loger.Println(err)
			}
		}
	}()
}

func publicServer(port string, address string) {
	listener, err := net.Listen("tcp", ":"+port)
	check(err)
	go func() {
		defer func() {
			err := recover()
			fatal(err, "publicServer "+port)
		}()

		for {
			conn, err := listener.Accept()
			check(err)

			go func() {
				defer func() {
					conn.Close()
					err := recover()
					fatal(err, "publicServer connection func")
				}()

				if tunnelSet == nil {
					fmt.Println("no connection!")
					loger.Println("no connection!")
					return
				}

				err := tunnelSet.ConnectTunnel(conn, []byte(address))
				if err != nil {
					fmt.Println(err)
					loger.Println(err)
				}
			}()
		}
	}()
}
