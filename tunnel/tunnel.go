package tunnel

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
)

var (
	MaxDataSize    = uint32(10000000)
	MaxMessageSize = uint32(4096)

	cHeaderCreate  = toInt("crea")
	cHeaderData    = toInt("data")
	cHeaderRead    = toInt("read")
	cHeaderReRead  = toInt("rere")
	cHeaderMessage = toInt("mesg")

	cMessageError = byte(1)
	cMessageState = byte(2)
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
	inReadNum  chan uint32
	inErr      chan error
}

type TunnelSet struct {
	tunnelLock  sync.Mutex
	count       uint64
	set         map[uint64]*tunnel
	creater     func(string) (io.ReadWriteCloser, error)
	outConnLock sync.Mutex
	outSendLock sync.Mutex
	outConn     io.Writer
	outErr      chan error
	closeChan   chan uint64
	messagePool chan message
}

func (this *TunnelSet) createTunnel() (t *tunnel) {
	t = &tunnel{
		inEmptyBuf: make(chan pack, 1),
		inFullBuf:  make(chan pack, 1),
		inMessage:  make(chan message, 256),
		inReadNum:  make(chan uint32, 1),
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

func (this *TunnelSet) sendMessage(tunnelId uint64, mtype byte, msg string) {
	this.outSendLock.Lock()
	defer this.outSendLock.Unlock()
	var err error
	defer func() {
		if err != nil {
			this.outErr <- err
		}
	}()

	err = binary.Write(this.outConn, binary.LittleEndian, headerError)
	if err != nil {
		return
	}

	err = binary.Write(this.outConn, binary.LittleEndian, tunnelId)
	if err != nil {
		return
	}

	buf := []byte(sendErr.Error())
	if len(buf) > int(ErrMaxSize) {
		buf = buf[:ErrMaxSize]
	}
	err = binary.Write(this.outConn, binary.LittleEndian, uint32(len(buf)))
	if err != nil {
		return
	}
	_, err = this.outConn.Write(buf)
}

func (this *TunnelSet) sendError(tunnelId uint64, sendErr error) {
	this.outSendLock.Lock()
	defer this.outSendLock.Unlock()
	var err error
	defer func() {
		if err != nil {
			this.outErr <- err
		}
	}()

	err = binary.Write(this.outConn, binary.LittleEndian, headerError)
	if err != nil {
		return
	}

	err = binary.Write(this.outConn, binary.LittleEndian, tunnelId)
	if err != nil {
		return
	}

	buf := []byte(sendErr.Error())
	if len(buf) > int(ErrMaxSize) {
		buf = buf[:ErrMaxSize]
	}
	err = binary.Write(this.outConn, binary.LittleEndian, uint32(len(buf)))
	if err != nil {
		return
	}
	_, err = this.outConn.Write(buf)
}

func (this *TunnelSet) sendData(p pack, tunnelId uint64) {
	this.outSendLock.Lock()
	defer this.outSendLock.Unlock()
	var err error
	defer func() {
		if err != nil {
			this.outErr <- err
		}
	}()

	err = binary.Write(this.outConn, binary.LittleEndian, headerData)
	if err != nil {
		return
	}

	err = binary.Write(this.outConn, binary.LittleEndian, p.readNum)
	if err != nil {
		return
	}

	if len(p.data) > int(MaxSize) {
		panic("pack too large!")
	}

	err = binary.Write(this.outConn, binary.LittleEndian, uint32(len(p.data)))
	if err != nil {
		return
	}
	_, err = this.outConn.Write(p.data)
}

func (this *TunnelSet) runTunnel(conn io.ReadWriter, tunnelId uint64, t *tunnel) (err error) {
	reciveSignal := make(chan error)
	go func() {
		var err error
		p := pack{
			data: make([]byte, 4096),
		}
		for {
			p.readNum = <-t.inReadNum

			n, err := conn.Read(p.data)
			if n != 0 {
				this.sendData(p, tunnelId)
			}
			if err == nil {
				break
			}
			if err != io.EOF {
				loger.Println(err)
			}
			break
		}
		reciveSignal <- err
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
