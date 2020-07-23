package main

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"runtime"
	"time"

	"github.com/fixiong/tunnel/common"
	"gopkg.in/yaml.v2"
)

type Configure struct {
	PublicPort string
	UpPort     string
	DownPort   string
	Secret     string
}

var (
	sendChan = make(chan common.Pack)
	exit     chan struct{}
	loger    *log.Logger
	config   = new(Configure)
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

	logFileName := "outside_" + time.Now().Format("2006-01-02") + ".log"
	logFile, err := os.Create(logFileName)
	check(err)

	loger = log.New(logFile, "outside_", log.Ldate|log.Ltime|log.Lshortfile)
	fmt.Println("logging to file: " + logFileName)

	<-exit
	loger.Println("exit!")
	fmt.Println("exit!")

}

func downServer() {
	listener, err := net.Listen("tcp", ":"+config.DownPort)
	check(err)
	go func() {
		defer func() {
			err := recover()
			fatal(err, "downServer ")
		}()

		heartbeatChan := make(chan struct{})

		go func() {
			for {
				heartbeatChan <- struct{}{}
				time.Sleep(time.Second)
			}
		}()

		for {
			err := func() (err error) {
				conn, err := listener.Accept()
				if err != nil {
					return
				}
				defer conn.Close()

				header := "tunnel down " + config.Secret
				header_buf := make([]byte, len(header))
				_, err = io.ReadFull(conn, header_buf)
				if err != nil {
					return
				}

				if header != string(header_buf) {
					err = errors.New("wrong down header!")
					return
				}

				common.OutConnectionStart(conn)
				return
			}()
			if err != nil {
				loger.Println(err)
			}
		}
	}()
}

func upServer() {
	listener, err := net.Listen("tcp", ":"+config.UpPort)
	check(err)
	go func() {
		defer func() {
			err := recover()
			fatal(err, "upServer ")
		}()

		for {
			err := func() (err error) {
				conn, err := listener.Accept()
				if err != nil {
					return
				}
				defer conn.Close()

				header := "tunnel up " + config.Secret
				header_buf := make([]byte, len(header))
				_, err = io.ReadFull(conn, header_buf)
				if err != nil {
					return
				}

				if header != string(header_buf) {
					err = errors.New("wrong up header!")
					return
				}

				for {
					err = func() (err error) {
						pack, err := common.GetPack(conn)
						if err != nil {
							common.PutReply(conn, err)
							return
						}
						switch pack.Command {
						case common.CCommandCreate:
							err = errors.New("wrong create tunnel!")
						case common.CCommandDelete:
							closeTunnel(pack.Tunnel)
						case common.CCommandData:
							err = sendContent(pack.Tunnel, pack.Content)
						case common.CCommandHeartbeat:
						default:
							err = errors.New("wrong command!")
						}
						err1 := common.PutReply(conn, err)
						if err == nil {
							err = err1
						}
						return
					}()
					if err != nil {
						return
					}
				}
			}()
			if err != nil {
				loger.Println(err)
			}
		}
	}()
}

func publicServer() {
	listener, err := net.Listen("tcp", ":"+config.PublicPort)
	check(err)
	go func() {
		defer func() {
			err := recover()
			fatal(err, "publicServer ")
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

				tunnelId, tunnel := newTunnel()
				defer deleteTunnel(tunnelId)

				reciveSignal := make(chan struct{})
				go func() {
					defer func() {
						err := recover()
						fatal(err, "publicServer recive func ")
					}()

					sendChan <- common.Pack{
						Tunnel:  tunnelId,
						Command: common.CCommandCreate,
					}

					for {
						if len(tunnel.FrontBuffer) == 0 {
							size := 4096
							tunnel.FrontBuffer = make([]byte, size)
						}
						n, err := conn.Read(tunnel.FrontBuffer)
						if n != 0 {
							sendChan <- common.Pack{
								Tunnel:  tunnelId,
								Content: tunnel.FrontBuffer[:n],
							}
							if n == len(tunnel.FrontBuffer) && n*2 < int(common.MaxSize) {
								tunnel.FrontBuffer = make([]byte, n*2)
							}
							tunnel.flip()
						}
						if err == nil {
							continue
						}
						if err != io.EOF {
							loger.Println(err)
						}
						break
					}
					reciveSignal <- struct{}{}
				}()

				sendSignal := make(chan struct{})
				go func() {
					defer func() {
						err := recover()
						fatal(err, "publicServer send func ")
					}()

					for {
						content := <-tunnel.ReciveChan
						if content.CloseFlag {
							break
						}

						n, err := conn.Read(tunnel.ReciveBuffer)
						if n != 0 {
							sendChan <- Pack{
								Tunnel:  tunnelId,
								Content: tunnel.ReciveBuffer[:n],
							}
						}
						if n == len(tunnel.ReciveBuffer) && n*2 < int(common.MaxSize) {
							tunnel.ReciveBuffer = make([]byte, n*2)
						}
						if err == nil {
							continue
						}
						if err != io.EOF {
							loger.Println(err)
						}
						break
					}
					sendSignal <- struct{}{}

				}()

				<-reciveSignal
			}()
		}
	}()
}
