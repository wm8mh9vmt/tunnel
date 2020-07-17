// cot_market project resource.go
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
	"sync"
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

type Content struct {
	Buf       []byte
	CloseFlag bool
}

type Tunnel struct {
	ReciveChan   chan Content
	ReciveBuffer []byte
}

type Pack struct {
	Tunnel  uint64
	Content []byte
}

var (
	tunnelCount = uint64(1)
	tunnelMap   map[uint64]*Tunnel
	tunnelLock  sync.Mutex
	sendChan    = make(chan Pack)
	reciveChan  = make(chan Pack)
	exit        chan struct{}
	loger       *log.Logger
	config      = new(Configure)
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

func newTunnel() (tunnelId uint64, tunnel *Tunnel) {
	tunnelLock.Lock()
	defer tunnelLock.Unlock()

	tunnelId = tunnelCount
	tunnelCount++

	tunnelMap[tunnelId] = &Tunnel{
		ReciveChan:   make(chan Content),
		ReciveBuffer: make([]byte, 4096),
	}
	return
}

func deleteTunnel(tunnelId uint64) {
	tunnelLock.Lock()
	defer tunnelLock.Unlock()

	delete(tunnelMap, tunnelId)
}

func closeTunnel(tunnelId uint64) {
	tunnelLock.Lock()
	defer tunnelLock.Unlock()

	tunnel, ok := tunnelMap[tunnelId]
	if !ok {
		return
	}

	tunnel.ReciveChan <- Content{
		CloseFlag: true,
	}
}

func sendContent(tunnelId uint64, content []byte) error {
	tunnelLock.Lock()
	defer tunnelLock.Unlock()

	tunnel, ok := tunnelMap[tunnelId]
	if !ok {
		return errors.New(common.CErrClosed)
	}

	tunnel.ReciveChan <- Content{
		Buf: content,
	}
	return nil
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
		var sending *Pack

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

				for {
					if sending == nil {
						select {
						case pack := <-sendChan:
							sending = &pack
						case <-heartbeatChan:
							err = common.PutPack(conn, 0, nil)
							if err != nil {
								return
							}
							continue
						}
					}
					err = common.PutPack(conn, sending.Tunnel, sending.Content)
					if err != nil {
						if err.Error() == common.CErrClosed {
							err = nil
							closeTunnel(sending.Tunnel)
						} else {
							return

						}
					}
					sending = nil
				}
			}
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
						tunnel, content, err := common.GetPack(conn)
						if err != nil {
							common.PutReply(conn, err)
							return
						}
						if len(content) == 0 {
							err = common.PutReply(conn, nil)
							return
						}

						common.PutReply(conn, sendContent(tunnel, content))
						return
					}()
					if err != nil {
						return
					}
				}
				return
			}
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
					fatal(err, "publicServer send func")
				}()

				tunnelId, tunnel := newTunnel()
				defer deleteTunnel(tunnelId)

				reciveSignal := make(chan struct{})
				go func() {
					defer func() {
						err := recover()
						fatal(err, "publicServer recive func ")
					}()

					reciveSignal <- struct{}{}

					conn.Read(tunnel.ReciveBuffer)
				}()

				<-reciveSignal
			}()
		}
	}()
}
