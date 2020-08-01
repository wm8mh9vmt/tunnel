package tunnel

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

var (
	MaxDataSize    = uint32(10000000)
	MaxMessageSize = uint32(4096)

	cHeaderCreate = toInt("crea")
	cHeaderData   = toInt("data")
	cHeaderError  = toInt("erro")
	cHeaderStat   = toInt("stat")
	cHeaderRead   = toInt("read")
)

func toInt(s string) uint32 {
	return binary.LittleEndian.Uint32([]byte(s))
}

type pack struct {
	readNum uint32
	data    []byte
}

type tunnel struct {
	inEmptyBuf chan pack
	inFullBuf  chan pack
	inError    chan error
	statSignal chan struct{}
	statNumber uint32
	readSignal chan struct{}
	readNumber uint32
	htbtSignal chan uint32
}

type TunnelSet struct {
	tunnelLock  sync.Mutex
	count       uint64
	set         map[uint64]*tunnel
	creater     func(string) (io.ReadWriteCloser, error)
	outConnLock sync.Mutex
	outSendLock sync.Mutex
	outConn     io.Writer
	bufferPool  chan []byte
	closeChan   chan uint64
}

func (this *TunnelSet) createTunnel() (t *tunnel) {
	t = &tunnel{
		inEmptyBuf: make(chan pack, 1),
		inFullBuf:  make(chan pack, 1),
		inError:    make(chan error, 1),
		statSignal: make(chan struct{}, 1),
		readSignal: make(chan struct{}, 1),
		htbtSignal: make(chan uint32, 1),
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
		count:      1,
		set:        make(map[uint64]*tunnel),
		creater:    creater,
		bufferPool: make(chan []byte, 4096),
		closeChan:  make(chan uint64),
	}
	this.outSendLock.Lock()
	return
}

func (this *TunnelSet) newBuf() (buf []byte) {
	select {
	case buf = <-this.bufferPool:
	default:
	}
	return
}

func (this *TunnelSet) deleteBuf(buf []byte) {
	select {
	case this.bufferPool <- buf:
	default:
	}
}

func (this *TunnelSet) sendHeader(header uint32, tunnelId uint64) (err error) {
	err = binary.Write(this.outConn, binary.LittleEndian, cHeaderError)
	if err != nil {
		return
	}

	err = binary.Write(this.outConn, binary.LittleEndian, tunnelId)
	return
}

func (this *TunnelSet) sendError(tunnelId uint64, sendErr error) (err error) {
	this.outSendLock.Lock()
	defer this.outSendLock.Unlock()

	err = this.sendHeader(cHeaderError, tunnelId)
	if err != nil {
		return
	}

	buf := []byte(sendErr.Error())
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

func (this *TunnelSet) reciveError(conn io.Reader, tunnelId uint64) (err error) {

	var length uint32
	err = binary.Read(conn, binary.LittleEndian, &length)
	if err != nil {
		return
	}
	if length > MaxMessageSize {
		err = errors.New("err too large!")
		return
	}
	buf := this.newBuf()
	defer this.deleteBuf(buf)
	if len(buf) < int(length) {
		buf = make([]byte, length)
	}
	_, err = io.ReadFull(conn, buf)
	if err != nil {
		return
	}

	t := this.set[tunnelId]
	if t != nil {
		select {
		case t.inError <- errors.New(string(buf)):
		default:
		}
	}
	return
}

func (this *TunnelSet) sendStat(tunnelId uint64, stat uint32) (err error) {
	this.outSendLock.Lock()
	defer this.outSendLock.Unlock()

	err = this.sendHeader(cHeaderStat, tunnelId)
	if err != nil {
		return
	}

	err = binary.Write(this.outConn, binary.LittleEndian, stat)
	if err != nil {
		return
	}
	return
}

func (this *TunnelSet) reciveStat(conn io.Reader, tunnelId uint64) (err error) {
	var stat uint32
	err = binary.Read(conn, binary.LittleEndian, &stat)
	if err != nil {
		return
	}

	t := this.set[tunnelId]
	if t == nil {
		go func() {
			this.sendError(tunnelId, errors.New("stat tunnel not found!"))
		}()
	} else {
		select {
		case t.statSignal <- struct{}{}:
			atomic.StoreUint32(&t.statNumber, stat)
		default:
		}
	}
	return
}

func (this *TunnelSet) sendRead(tunnelId uint64, read uint32) (err error) {
	this.outSendLock.Lock()
	defer this.outSendLock.Unlock()

	err = this.sendHeader(cHeaderRead, tunnelId)
	if err != nil {
		return
	}

	err = binary.Write(this.outConn, binary.LittleEndian, read)
	if err != nil {
		return
	}
	return
}

func (this *TunnelSet) reciveRead(conn io.Reader, tunnelId uint64) (err error) {
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
		case t.readSignal <- struct{}{}:
			atomic.StoreUint32(&t.readNumber, readNum)
		default:
			fmt.Println("readNum duplicated!")
		}
	}
	return
}

func (this *TunnelSet) sendData(tunnelId uint64, p pack) (err error) {
	this.outSendLock.Lock()
	defer this.outSendLock.Unlock()

	err = this.sendHeader(cHeaderData, tunnelId)
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

func (this *TunnelSet) reciveData(conn io.Reader, tunnelId uint64) (err error) {
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
	if length > MaxDataSize {
		err = errors.New("pack too large!")
		return
	}

	t := this.set[tunnelId]
	if t == nil {
		go func() {
			this.sendError(tunnelId, errors.New("data tunnel not found!"))
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
	err := this.sendError(tunnelId, sendErr)
	for err != nil {
		fmt.Println(err)
		time.Sleep(time.Second)
		err = this.sendError(tunnelId, sendErr)
	}
}

func (this *TunnelSet) sendReadLoop(tunnelId uint64, read uint32) {
	err := this.sendRead(tunnelId, read)
	for err != nil {
		fmt.Println(err)
		time.Sleep(time.Second)
		err = this.sendRead(tunnelId, read)
	}
}

func (this *TunnelSet) reciveCreate(conn io.Reader, tunnelId uint64) (err error) {
	var length uint32
	err = binary.Read(conn, binary.LittleEndian, &length)
	if err != nil {
		return
	}
	if length > MaxMessageSize {
		err = errors.New("command too large!")
		return
	}

	buf := this.newBuf()
	defer this.deleteBuf(buf)
	if len(buf) < int(length) {
		buf = make([]byte, length)
	}
	_, err = io.ReadFull(conn, buf)
	if err != nil {
		return
	}

	if this.creater == nil {
		err = errors.New("not slave tunnelSet!")
		return
	}

	subConn, subErr := this.creater(string(buf))
	if subErr != nil {
		go func() {
			this.sendErrorLoop(tunnelId, subErr)
		}()
	} else {
		t := this.set[tunnelId]
		if t != nil {
			select {
			case t.inError <- errors.New("tunnel overwrited!"):
			default:
			}
		}
		t = this.createTunnel()
		this.set[tunnelId] = t
		go func() {
			defer func() {
				this.closeChan <- tunnelId
			}()
			err := this.runTunnel(subConn, tunnelId, t)
			if err != nil {
				fmt.Println(err)
			}
		}()
	}
	return
}

func (this *TunnelSet) runTunnel(conn io.ReadWriteCloser, tunnelId uint64, t *tunnel) (err error) {
	defer func() {
		close(t.readSignal)
		close(t.statSignal)
		close(t.inError)
		close(t.inFullBuf)
		close(t.inEmptyBuf)
		conn.Close()
	}()

	go func() {
		next_htbt := func(stat uint32) {
			time.Sleep(time.Second * 10)
			t.htbtSignal <- stat
		}
		var n int
		p := pack{
			data:    make([]byte, 4096),
			readNum: 0,
		}
		var msg [4]byte
		for {
			select {
			case htbt := <-t.htbtSignal:
				if htbt == p.readNum {
					err1 := this.sendStat(tunnelId, htbt)
					if err1 != nil {
						fmt.Println(err1)
					}
					go next_htbt(htbt)
				}
			case <-t.readSignal:
				readNum := atomic.LoadUint32(&t.readNumber)
				if p.readNum != readNum {
					n, err = conn.Read(p.data)
					p.readNum = readNum
				}

				if n != 0 {
					this.sendDataLoop(tunnelId, p)
					go next_htbt(p.readNum)
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
					t.inError <- err
					return
				}
			}
		}
	}()

	readNum := uint32(1)
	this.sendReadLoop(tunnelId, readNum)
	for {
		select {
		case err = <-t.inError:
			select {
			case p := <-t.inFullBuf:
				conn.Write(p.data)
			default:
			}
			return
		case p := <-t.inFullBuf:
			if len(p.data) == 0 {
				return
			}
			if p.readNum == readNum {
				_, err = conn.Write(p.data)
				if err != nil {
					this.sendErrorLoop(tunnelId, err)
					return
				}
				readNum++
				this.sendReadLoop(tunnelId, readNum)
			} else {
				fmt.Println("unknown read number:", p.readNum)
			}
		case <-t.statSignal:
			ir := atomic.LoadUint32(&t.statNumber)
			if ir == readNum {
				this.sendReadLoop(tunnelId, readNum)
			}
		}
	}
}

func (this *TunnelSet) Connect(conn io.ReadWriter) (err error) {
	this.outConnLock.Lock()
	defer this.outConnLock.Unlock()
	this.outConn = conn
	this.outSendLock.Unlock()
	defer this.outSendLock.Lock()

	for {
		select {
		case id := <-this.closeChan:
			delete(this.set, id)
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
		case cHeaderCreate:
			err = this.reciveCreate(conn, tunnelId)
			if err != nil {
				return
			}

		case cHeaderRead:
			err = this.reciveRead(conn, tunnelId)
			if err != nil {
				return
			}

		case cHeaderStat:
			err = this.reciveStat(conn, tunnelId)
			if err != nil {
				return
			}

		case cHeaderData:
			err = this.reciveData(conn, tunnelId)
			if err != nil {
				return
			}

		case cHeaderError:
			err = this.reciveError(conn, tunnelId)
			if err != nil {
				return
			}
		default:
			err = errors.New("unknown header!")
			return
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
