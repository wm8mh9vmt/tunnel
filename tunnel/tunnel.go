package tunnel

import (
	"encoding/binary"
	"errors"
	"io"
	"sync"
)

var (
	MaxSize      = uint32(10000000)
	ReplyMaxSize = uint32(4096)

	headerData      = toInt("data")
	headerRead      = toInt("read")
	headerHeartbeat = toInt("htbt")
	headerError     = toInt("erro")
)

func toInt(s string) uint32 {
	return binary.LittleEndian.Uint32([]byte(s))
}

type tunnel struct {
	readCount uint32
	sendChan  chan uint32
	outSignal chan uint32
	inSignal  chan struct{}
	inBuffer  []byte
	inNumber  uint32
}

func (this *tunnel) readToBuffer(inNumber uint32, size int, conn io.Reader) (err error) {

	select {
	case <-this.inSignal:

		this.inNumber = inNumber
		if len(this.inBuffer) < size {
			this.inBuffer = make([]byte, size)
		}
		_, err = io.ReadFull(conn, this.inBuffer)
		if err != nil {
			return
		}

	default:
		err = errors.New("read data not flushed!")
		return
	}

	return
}

type pack struct {
	header  uint32
	number  uint32
	tunnel  uint64
	content []byte
}

type TunnelSet struct {
	count    uint64
	set      map[uint64]*tunnel
	lock     sync.Mutex
	creater  func() (io.ReadWriteCloser, error)
	sendChan chan pack
	sending  pack
}

type Connection struct {
	set       *TunnelSet
	errSignal chan error
	conn      io.ReadWriter
}

func CreateTunnelSet(creater func() (io.ReadWriteCloser, error)) *TunnelSet {
	return &TunnelSet{
		count:    1,
		set:      make(map[uint64]*tunnel),
		creater:  creater,
		sendChan: make(chan pack),
	}
}

func CreateConnection(set *TunnelSet, conn io.ReadWriter) (tunnelConn *Connection) {
	tunnelConn = &Connection{
		set:       set,
		errSignal: make(chan error),
		conn:      conn,
	}
	return
}

func (this *TunnelSet) RunTunnel(conn io.ReadWriter) (err error) {
	t := this.createMasterTunnel(conn)

	reciveSignal := make(chan error)
	go func() {
		buffer := make([]byte, 4096)
		for {
			n, err := conn.Read(buffer)
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
}

func (this *TunnelSet) createMasterTunnel(conn io.ReadWriteCloser) *tunnel {
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

func (this *Connection) getTunnel(create bool) (t *tunnel, err error) {
	var tunnelId uint64
	err = binary.Read(this.conn, binary.LittleEndian, &tunnelId)
	if err != nil {
		return
	}

	this.set.lock.Lock()
	defer this.set.lock.Unlock()

	t, ok := this.set.set[tunnelId]
	if ok {
		return
	}
	if !create {
		err = errors.New("tunnel not found!")
		return
	}

	t = this.set.createTunnel(tunnelId)
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

func (this *Connection) Run() (err error) {
	for {
		if this.set.sending.header != 0 {
			err = this.putPack(this.set.sending)
			if err != nil {
				return
			}
			this.set.sending = pack{}
		}
		select {
		case err = <-this.errSignal:
			return err
		case this.set.sending = <-this.set.sendChan:
		}
	}
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
