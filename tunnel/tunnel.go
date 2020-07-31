package tunnel

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

var (
	MaxDataSize    = uint32(10000000)
	MaxMessageSize = uint32(4096)

	cHeaderCreate  = toInt("crea")
	cHeaderData    = toInt("data")
	cHeaderMessage = toInt("mesg")

	cMessageError = byte(1)
	cMessageState = byte(2)
	cMessageRead  = byte(3)
)

func toInt(s string) uint32 {
	return binary.LittleEndian.Uint32([]byte(s))
}

type pack struct {
	readNum uint32
	data    []byte
}

type message struct {
	msgType byte
	data    []byte
}

type tunnel struct {
	inEmptyBuf chan pack
	inFullBuf  chan pack
	inMessage  chan message
}

type TunnelSet struct {
	tunnelLock  sync.Mutex
	count       uint64
	set         map[uint64]*tunnel
	creater     func(string) (io.ReadWriteCloser, error)
	outConnLock sync.Mutex
	outSendLock sync.Mutex
	outConn     io.Writer
	closeChan   chan uint64
	messagePool chan message
}

func (this *TunnelSet) createTunnel() (t *tunnel) {
	t = &tunnel{
		inEmptyBuf: make(chan pack, 1),
		inFullBuf:  make(chan pack, 1),
		inMessage:  make(chan message, 256),
	}
	t.inEmptyBuf <- pack{
		data: make([]byte, 4096),
	}

	return
}

func (this *TunnelSet) CreateMasterTunnel() (t *tunnel) {
	if this.creater != nil {
		panic("need master tunnelSet!")
	}
	tunnelId := this.count
	this.count++

	return
}

func CreateTunnelSet(creater func(string) (io.ReadWriteCloser, error)) (this *TunnelSet) {
	this = &TunnelSet{
		count:       1,
		set:         make(map[uint64]*tunnel),
		creater:     creater,
		closeChan:   make(chan uint64),
		messagePool: make(chan message, 4096),
	}
	this.outSendLock.Lock()
	return
}

func (this *TunnelSet) sendMessage(tunnelId uint64, mtype byte, buf []byte) (err error) {
	this.outSendLock.Lock()
	defer this.outSendLock.Unlock()

	err = binary.Write(this.outConn, binary.LittleEndian, cHeaderMessage)
	if err != nil {
		return
	}

	err = binary.Write(this.outConn, binary.LittleEndian, tunnelId)
	if err != nil {
		return
	}

	err = binary.Write(this.outConn, binary.LittleEndian, mtype)
	if err != nil {
		return
	}

	if len(buf) > int(MaxMessageSize) {
		buf = buf[:MaxMessageSize]
	}
	err = binary.Write(this.outConn, binary.LittleEndian, uint32(len(buf)))
	if err != nil {
		return
	}
	_, err = this.outConn.Write(buf)
	return
}

func (this *TunnelSet) sendData(tunnelId uint64, p pack) (err error) {
	this.outSendLock.Lock()
	defer this.outSendLock.Unlock()

	err = binary.Write(this.outConn, binary.LittleEndian, cHeaderData)
	if err != nil {
		return
	}

	err = binary.Write(this.outConn, binary.LittleEndian, p.readNum)
	if err != nil {
		return
	}

	if len(p.data) > int(MaxDataSize) {
		panic("pack too large!")
	}

	err = binary.Write(this.outConn, binary.LittleEndian, uint32(len(p.data)))
	if err != nil {
		return
	}
	_, err = this.outConn.Write(p.data)
	return
}

func (this *TunnelSet) sendDataLoop(tunnelId uint64, p pack) {
	err := this.sendData(tunnelId, p)
	for err != nil {
		fmt.Println(err)
		time.Sleep(time.Second)
		err = this.sendData(tunnelId, p)
	}
}

func (this *TunnelSet) sendErrorLoop(tunnelId uint64, sendErr error) {
	err := this.sendMessage(tunnelId, cMessageError, []byte(sendErr.Error()))
	for err != nil {
		fmt.Println(err)
		time.Sleep(time.Second)
		err1 := this.sendMessage(tunnelId, cMessageError, []byte(sendErr.Error()))
	}
}

func (this *TunnelSet) sendReadLoop(tunnelId uint64, stat uint32) {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], stat)
	err := this.sendMessage(tunnelId, cMessageRead, buf[:])
	for err != nil {
		fmt.Println(err)
		time.Sleep(time.Second)
		err = this.sendMessage(tunnelId, cMessageRead, buf[:])
	}
}

type readInfo struct {
	rType  uint32
	number uint32
}

type statInfo struct {
	sType  uint32
	number uint32
}

var (
	cRTypeRead = uint32(1)
	cRTypeHtbt = uint32(2)

	cSTypeStat = uint32(1)
)

func (this *TunnelSet) runTunnel(conn io.ReadWriteCloser, tunnelId uint64, t *tunnel) (err error) {
	defer func() {
		close(t.inMessage)
		close(t.inFullBuf)
		close(t.inEmptyBuf)
		conn.Close()
	}()

	readChan := make(chan readInfo, 1)
	defer close(readChan)
	go func() {
		next_stat := func(stat uint32) {
			time.Sleep(time.Second * 10)
			readChan <- readInfo{
				rType:  1,
				number: stat,
			}
		}
		var err error
		var n int
		p := pack{
			data:    make([]byte, 4096),
			readNum: 0,
		}
		for {
			info := <-readChan
			switch info.rType {
			case cRTypeHtbt:
				if info.number == p.readNum {
					var msg [4]byte
					binary.LittleEndian.PutUint32(msg[:], p.readNum)
					err1 := this.sendMessage(tunnelId, cMessageState, msg[:])
					if err1 != nil {
						fmt.Println(err1)
					}
					go next_stat(p.readNum)
				}
			case cRTypeRead:
				if p.readNum != info.number {
					n, err = conn.Read(p.data)
					p.readNum = info.number
				}

				if n != 0 {
					this.sendDataLoop(tunnelId, p)
					go next_stat(p.readNum)
					if n == len(p.data) && n < int(MaxDataSize) {
						ndata := make([]byte, n*2)
						for i, c := range p.data {
							ndata[i] = c
						}
						p.data = ndata
					}
				}

				if err != nil {
					this.sendErrorLoop(tunnelId, err)
					if err != io.EOF {
						fmt.Println(err)
					}
					return
				}
			default:
				return
			}
		}
	}()

	statChan := make(chan statInfo, 1)
	defer close(statChan)
	go func() {
		defer func() {
			statChan <- statInfo{}
		}()

		for {
			err := func() (err error) {
				msg := <-t.inMessage
				defer func() {
					select {
					case this.messagePool <- msg:
					default:
					}
				}()

				switch msg.msgType {
				case cMessageError:
					err = errors.New(string(msg.data))
					return
				case cMessageRead:
					if len(msg.data) != 4 {
						err = errors.New("wrong read info size!")
						return
					}
					readChan <- readInfo{
						number: binary.LittleEndian.Uint32(msg.data),
					}
				case cMessageState:
					if len(msg.data) != 4 {
						err = errors.New("wrong state info size!")
						return
					}
					statChan <- statInfo{
						number: binary.LittleEndian.Uint32(msg.data),
					}
				default:
					err = io.EOF
					return
				}
			}()
			if err != nil {
				if err.Error() != "EOF" {
					fmt.Println(err)
				}
				break
			}
		}
	}()

	readNum := uint32(1)
	this.sendReadLoop(tunnelId, readNum)
	for {
		select {
		case p := <-t.inFullBuf:
			if len(p.data) == 0 {
				return
			}
			if p.readNum == readNum {
				_, err := conn.Write(p.data)
				if err != nil {
					this.sendErrorLoop(tunnelId, err)
					if err != io.EOF {
						fmt.Println(err)
					}
					return
				}
				readNum++
				this.sendReadLoop(tunnelId, readNum)
			} else {
				fmt.Println("unknown read number:", p.readNum)
			}
		case s := <-statChan:
			switch s.sType {
			case cSTypeStat:
				if s.number == readNum {
					this.sendReadLoop(tunnelId, readNum)
				}
			default:
				select {
				case p := <-t.inFullBuf:
					conn.Write(p.data)
				default:
				}
				return
			}
		}
	}
}

func (this *TunnelSet) Connect(conn io.ReadWriter) (err error) {
	this.outConnLock.Lock()
	defer this.outConnLock.Unlock()
	this.outConn = conn
	this.outErr = make(chan error)
	this.outSendLock.Unlock()
	defer this.outSendLock.Lock()
	defer close(this.outErr)

	for {

		select {
		case id := <-this.closeChan:
			delete(this.set, id)
		case err = <-this.outErr:
			return
		default:
		}

		var header uint32
		err = binary.Read(conn, binary.LittleEndian, &header)
		if err != nil {
			return
		}

		var tunnelId uint64
		err = binary.Read(conn, binary.LittleEndian, &tunnelId)
		if err != nil {
			return
		}

		switch header {
		case headerHeartbeat:
			continue
		case headerCreate:
			var length uint32
			err = binary.Read(conn, binary.LittleEndian, &length)
			if err != nil {
				return
			}
			if length > CommandMaxSize {
				err = errors.New("command too large!")
				return
			}

			cmdbuf := make([]byte, length)
			_, err = io.ReadFull(conn, cmdbuf)
			if err != nil {
				return
			}

			if this.creater == nil {
				err = errors.New("not slave tunnelSet!")
				return
			}

			subConn, subErr := this.creater(string(cmdbuf))
			if subErr != nil {
				go func() {
					this.sendError(tunnelId, subErr)
				}()
			} else {
				t := this.set[tunnelId]
				if t != nil {
					select {
					case t.inErr <- errors.New("tunnel overwrited!"):
					default:
					}
				}
				t = this.createTunnel()
				this.set[tunnelId] = t
				go func() {
					t.run(subConn)
				}()
			}

		case headerRead:
			var readNum uint32
			err = binary.Read(conn, binary.LittleEndian, &readNum)
			if err != nil {
				return
			}

			t := this.set[tunnelId]
			if t == nil {
				go func() {
					this.sendError(tunnelId, errors.New("read tunnel not found!"))
				}()
			} else {
				select {
				case t.inReadNum <- readNum:
				default:
					fmt.Println("readNum duplicated!")
				}
			}

		case headerData:
			var readNum uint32
			err = binary.Read(conn, binary.LittleEndian, &readNum)
			if err != nil {
				return
			}

			var length uint32
			err = binary.Read(conn, binary.LittleEndian, &length)
			if err != nil {
				return
			}
			if length > MaxSize {
				err = errors.New("pack too large!")
				return
			}

			t := this.set[tunnelId]
			if t == nil {
				go func() {
					this.sendError(tunnelId, errors.New("read tunnel not found!"))
				}()
			} else {
				select {
				case p := <-t.inEmptyBuf:

					if len(p.data) < int(length) {
						p.data = make([]byte, length)
					}

					_, err = io.ReadFull(conn, p.data)
					if err != nil {
						return
					}
					p.readNum = readNum

					select {
					case t.inFullBuf <- p:

					default:

						select {
						case t.inEmptyBuf <- p:
						default:
						}

						fmt.Println("buffer chan full!")
					}
				default:

					buf := make([]byte, length)
					_, err = io.ReadFull(conn, buf)
					if err != nil {
						return
					}
					fmt.Println("out of buffer!")
				}
			}
		case headerError:
			var length uint32
			err = binary.Read(conn, binary.LittleEndian, &length)
			if err != nil {
				return
			}
			if length > ErrMaxSize {
				err = errors.New("err too large!")
				return
			}
			errbuf := make([]byte, length)
			_, err = io.ReadFull(conn, errbuf)
			if err != nil {
				return
			}

			t := this.set[tunnelId]
			if t != nil {
				select {
				case t.inErr <- errors.New(string(errbuf)):
				default:
				}
			}
		}
	}
}

func (this *TunnelSet) createMasterTunnel() *tunnel {
	this.lock.Lock()
	defer this.lock.Unlock()
	tunnelId := this.count
	this.count++

	this.set[tunnelId] = &tunnel{
		conn: conn,
	}

	this.dataChan <- pack{
		header: headerRead,
	}
	return
}

func (this *Connection) getPack() (err error) {
	var header uint32
	err = binary.Read(this.conn, binary.LittleEndian, &header)
	if err != nil {
		return
	}

	var t *tunnel
	switch header {
	case headerHeartbeat:
		return
	case headerRead:
		t, err = this.getTunnel()
		if err != nil {
			return
		}

		var readNumber uint32
		err = binary.Read(this.conn, binary.LittleEndian, &readNumber)
		if err != nil {
			return
		}
		select {
		case t.outSignal <- readNumber:
		default:
			err = errors.New("out buffer not flushed!")
		}
	case headerData:
		t, err = this.getTunnel()
		if err != nil {
			return
		}
		var length uint32
		err = binary.Read(this.conn, binary.LittleEndian, &length)
		if err != nil {
			return
		}
		if length > MaxSize {
			err = errors.New("pack too large!")
			return
		}
		err = t.readToInBuf(int(length), this.conn)
		if err != nil {
			return
		}
	}

	if header == headerHeartbeat {
		return
	}

	var length uint32
	err = binary.Read(this.conn, binary.LittleEndian, &length)
	if err != nil {
		return
	}

	if length > MaxSize {
		err = errors.New("pack too large!")
		return
	}

	content = make([]byte, length)
	_, err = io.ReadFull(conn, pack.Content)
	return
	pack, err := GetPack(conn)
	if err != nil {
		PutReply(conn, err)
		return
	}
	switch pack.Command {
	case CCommandCreate:
		err = errors.New("wrong create tunnel!")
	case CCommandDelete:
		closeTunnel(pack.Tunnel)
	case CCommandData:
		err = sendContent(pack.Tunnel, pack.Content)
	case CCommandHeartbeat:
	default:
		err = errors.New("wrong command!")
	}
	err1 := PutReply(conn, err)
	if err == nil {
		err = err1
	}
	return
}

func (this *Connection) putPack(p pack) (err error) {
	err = binary.Write(this.conn, binary.LittleEndian, p.header)
	if err != nil {
		return
	}

	length := uint32(len(p.content))

	err = binary.Write(this.conn, binary.LittleEndian, length)
	if err != nil {
		return
	}

	if length != 0 {
		_, err = this.conn.Write(p.content)
	}
	return
}

func (this *TunnelConnection) CreateTunnel(conn io.ReadWriteCloser) (tunnelId uint64) {
	this.tunnelLock.Lock()
	defer this.tunnelLock.Unlock()

	tunnelId = this.tunnelCount
	this.tunnelCount++

	this.tunnelMap[tunnelId] = &tunnel{
		conn: conn,
	}

	this.dataChan <- pack{
		header: headerRead,
	}
	return
}

func newTunnel(conn io.ReadWriteCloser) (tunnelId uint64, tunnel *Tunnel) {

	tunnelId = tunnelCount
	tunnelCount++

	tunnelMap[tunnelId] = &Tunnel{
		Connetion: conn,
	}
	return
}

func deleteTunnel(tunnelId uint64) {
	tunnelLock.Lock()
	defer tunnelLock.Unlock()

	tunnel, ok := tunnelMap[tunnelId]
	if !ok {
		return
	}
	if tunnel.Connetion != nil {
		tunnel.Connetion.Close()
		tunnel.Connetion = nil
	}
	delete(tunnelMap, tunnelId)
}
func putHeartbeat() (err error) {
	outLock.Lock()
	defer outLock.Unlock()

	_, err = outConnection.Write([]byte(headerHeartbeat))
	if err != nil {
		return
	}

	_, err = getReply(outConnection)
	return
}

func OutConnectionStart(conn io.ReadWriter) (err error) {
	outConnection = conn
	outSignal = make(chan struct{})
}
