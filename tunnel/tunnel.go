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

	errNotFound     = errors.New("tunnel not found!")
	ErrNoConnection = errors.New("no connection!")
)

func toInt(s string) uint32 {
	return binary.LittleEndian.Uint32([]byte(s))
}

type pack struct {
	readNum uint32
	size    uint32
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
	tunnelMap   sync.Map
	creater     func([]byte) (io.ReadWriteCloser, error)
	outConnLock sync.Mutex
	outSendLock sync.Mutex
	outConn     io.Writer
	bufferPool  chan []byte
	DummyBuf    []byte
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

func CreateTunnelSet(creater func([]byte) (io.ReadWriteCloser, error)) (this *TunnelSet) {
	this = &TunnelSet{
		count:      1,
		creater:    creater,
		bufferPool: make(chan []byte, 4096),
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
	err = binary.Write(this.outConn, binary.LittleEndian, header)
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

func (this *TunnelSet) reciveError(conn io.Reader, t *tunnel) (err error) {

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
	defer func() {
		this.deleteBuf(buf) //capture by pointer!
	}()
	if len(buf) < int(length) {
		buf = make([]byte, length)
	}
	_, err = io.ReadFull(conn, buf[:length])
	if err != nil {
		return
	}

	if t == nil {
		return
	}

	select {
	case t.inError <- errors.New(string(buf)):
	default:
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

func (this *TunnelSet) reciveStat(conn io.Reader, t *tunnel) (err error) {
	var stat uint32
	err = binary.Read(conn, binary.LittleEndian, &stat)
	if err != nil {
		return
	}

	if t == nil {
		err = errNotFound
		return
	}

	select {
	case t.statSignal <- struct{}{}:
	default:
		fmt.Println("stat number overwrited!")
	}
	atomic.StoreUint32(&t.statNumber, stat)
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

func (this *TunnelSet) reciveRead(conn io.Reader, t *tunnel) (err error) {
	var readNum uint32
	err = binary.Read(conn, binary.LittleEndian, &readNum)
	if err != nil {
		return
	}

	if t == nil {
		err = errNotFound
		return
	}

	select {
	case t.readSignal <- struct{}{}:
	default:
		fmt.Println("readNum overwrited!")
	}
	atomic.StoreUint32(&t.readNumber, readNum)
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

	if p.size > MaxDataSize {
		panic("pack too large!")
	}

	err = binary.Write(this.outConn, binary.LittleEndian, p.size)
	if err != nil {
		return
	}
	_, err = this.outConn.Write(p.data[:p.size])
	return
}

func (this *TunnelSet) reciveData(conn io.Reader, t *tunnel) (err error) {
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

	if t == nil {
		if len(this.DummyBuf) < int(length) {
			this.DummyBuf = make([]byte, length)
		}
		_, err = io.ReadFull(conn, this.DummyBuf[:length])
		if err == nil {
			err = errNotFound
		}
		return
	}

	select {
	case p := <-t.inEmptyBuf:

		if len(p.data) < int(length) {
			p.data = make([]byte, length)
		}
		p.size = length
		p.readNum = readNum

		_, err = io.ReadFull(conn, p.data[:p.size])
		if err != nil {
			return
		}

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

		if len(this.DummyBuf) < int(length) {
			this.DummyBuf = make([]byte, length)
		}
		_, err = io.ReadFull(conn, this.DummyBuf[:length])
		fmt.Println("out of buffer!")
	}
	return
}

func (this *TunnelSet) sendCreate(tunnelId uint64, cmd []byte) (err error) {
	this.outSendLock.Lock()
	defer this.outSendLock.Unlock()

	err = this.sendHeader(cHeaderError, tunnelId)
	if err != nil {
		return
	}

	if len(cmd) > int(MaxMessageSize) {
		panic("create command too long!")
	}
	err = binary.Write(this.outConn, binary.LittleEndian, uint32(len(cmd)))
	if err != nil {
		return
	}
	_, err = this.outConn.Write(cmd)
	return
}

func (this *TunnelSet) reciveCreate(conn io.Reader, t *tunnel, tunnelId uint64) (err error) {
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
	defer func() {
		this.deleteBuf(buf)
	}()
	if len(buf) < int(length) {
		buf = make([]byte, length)
	}
	_, err = io.ReadFull(conn, buf[:length])
	if err != nil {
		return
	}

	if this.creater == nil {
		err = errors.New("not slave tunnelSet!")
		return
	}

	subConn, subErr := this.creater(buf)
	if subErr != nil {
		go func() {
			this.sendErrorLoop(tunnelId, subErr)
		}()
	} else {
		if t != nil {
			select {
			case t.inError <- errors.New("tunnel overwrited!"):
			default:
			}
		}
		t = this.createTunnel()
		this.tunnelMap.Store(tunnelId, t)
		go func() {
			defer this.tunnelMap.Delete(tunnelId)
			err := this.runTunnel(subConn, tunnelId, t)
			if err != nil && err.Error() != "EOF" {
				fmt.Println(err)
			}
		}()
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

func (this *TunnelSet) sendCreateLoop(tunnelId uint64, cmd []byte) {
	err := this.sendCreate(tunnelId, cmd)
	for err != nil {
		fmt.Println(err)
		time.Sleep(time.Second)
		err = this.sendCreate(tunnelId, cmd)
	}
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
		var err error
		next_htbt := func(stat uint32) {
			time.Sleep(time.Second * 10)
			t.htbtSignal <- stat
		}
		p := pack{
			data:    make([]byte, 4096),
			readNum: 0,
		}
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
					length, err1 := conn.Read(p.data)
					err = err1
					p.readNum = readNum
					p.size = uint32(length)
				}

				if p.size != 0 {
					this.sendDataLoop(tunnelId, p)
					go next_htbt(p.readNum)
					if int(p.size) == len(p.data) && p.size < MaxDataSize {
						ndata := make([]byte, p.size*2)
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
	defer func() {
		this.outConn = nil
	}()
	this.outSendLock.Unlock()
	defer this.outSendLock.Lock()

	for {
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

		var t *tunnel
		ti, ok := this.tunnelMap.Load(tunnelId)
		if ok {
			t = ti.(*tunnel)
		}

		switch header {
		case cHeaderCreate:
			err = this.reciveCreate(conn, t, tunnelId)
		case cHeaderRead:
			err = this.reciveRead(conn, t)
		case cHeaderStat:
			err = this.reciveStat(conn, t)
		case cHeaderData:
			err = this.reciveData(conn, t)
		case cHeaderError:
			err = this.reciveError(conn, t)
		default:
			err = errors.New("unknown header!")
		}

		if err != nil {
			if err == errNotFound {
				go this.sendErrorLoop(tunnelId, err)
				err = nil
			} else {
				return
			}
		}
	}
}

func (this *TunnelSet) ConnectTunnel(conn io.ReadWriteCloser, cmd []byte) (err error) {
	if this.creater != nil {
		err = errors.New("not master tunnel!")
	}
	if this.outConn == nil {
		err = ErrNoConnection
		return
	}

	tunnelId := atomic.AddUint64(&this.count, 1)
	t := this.createTunnel()
	this.tunnelMap.Store(tunnelId, t)
	defer this.tunnelMap.Delete(tunnelId)

	this.sendCreateLoop(tunnelId, cmd)

	err = this.runTunnel(conn, tunnelId, t)
	if err != nil && err.Error() == "EOF" {
		err = nil
	}
	return
}
