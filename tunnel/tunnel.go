package tunnel

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
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
	ret := binary.LittleEndian.Uint32([]byte(s))
	return ret + 0x8edf8a0e
}

type pack struct {
	readNum uint32
	size    uint32
	data    []byte
}

type tunnel struct {
	inEmptyBuf chan pack
	inFullBuf  chan pack
	inError_   chan error
	statSignal chan uint32
	readSignal chan uint32
	heartbeat  chan time.Time
}

func (this *tunnel) sendError(err error) {
	select {
	case this.inError_ <- err:
	default:
	}
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
	dummyBuf_   []byte
	closed      bool
}

func (this *TunnelSet) createTunnel() (t *tunnel) {
	t = &tunnel{
		inEmptyBuf: make(chan pack, 1),
		inFullBuf:  make(chan pack, 1),
		inError_:   make(chan error, 1),
		statSignal: make(chan uint32, 1),
		readSignal: make(chan uint32, 1),
		heartbeat:  make(chan time.Time, 1),
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
	go func() {
		for {
			if this.closed {
				return
			}
			time.Sleep(time.Second * 10)
			if this.outConn == nil {
				continue
			}
			now := time.Now()

			this.tunnelMap.Range(func(_, value interface{}) bool {
				t := value.(*tunnel)
				select {
				case t.heartbeat <- now:
				default:
				}
				return true
			})
		}

	}()
	return
}

func (this *TunnelSet) Close() {
	this.tunnelMap.Range(func(_, value interface{}) bool {
		t := value.(*tunnel)
		select {
		case t.inError_ <- errors.New("tunnel set closed"):
		default:
		}
		this.closed = true
		return true
	})
}

func (this *TunnelSet) getDummy(size int) []byte {
	if len(this.dummyBuf_) < size {
		this.dummyBuf_ = make([]byte, size)
	}
	return this.dummyBuf_
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
	if sendErr == nil {
		panic("can't send a nil error!")
	}
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
	buf_ := this.newBuf()
	defer func() {
		this.deleteBuf(buf_) //capture by pointer!
	}()
	if len(buf_) < int(length) {
		buf_ = make([]byte, length)
	}
	buf := buf_[:length]

	_, err = io.ReadFull(conn, buf)
	if err != nil {
		return
	}

	sendErr := errors.New(string(buf))
	if t == nil {
		if sendErr.Error() != "EOF" {
			fmt.Println("err ignored! ", sendErr)
		}
		return
	}

	t.sendError(sendErr)
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
	case t.statSignal <- stat:
	default:
		fmt.Println("stat number overwrited:", stat)
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

func (this *TunnelSet) reciveRead(conn io.Reader, t *tunnel) (err error) {
	var readNum uint32
	err = binary.Read(conn, binary.LittleEndian, &readNum)
	if err != nil {
		return
	}
	//fmt.Println("recive read number:", readNum)

	if t == nil {
		err = errNotFound
		return
	}

	select {
	case t.readSignal <- readNum:
	default:
		fmt.Println("readNum overwrited:", readNum)
	}
	return
}

func debug_buf(tunnelId uint64, buf []byte) {
	ps := len(buf)
	var tail string
	if ps > 40 {
		ps = 40
		tail = "..."
	}
	fmt.Println("tunnel ", tunnelId, " send", len(buf), "bytes, data:", string(buf[:ps]), tail)
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
	//debug_buf(tunnelId, p.data[:p.size])
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
		_, err = io.ReadFull(conn, this.getDummy(int(length)))
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
		fmt.Println("out of buffer!", length)
		tmp := make([]byte, length)
		_, err = io.ReadFull(conn, tmp)
		fmt.Println("out of buffer!")
	}
	return
}

func (this *TunnelSet) sendCreate(tunnelId uint64, cmd []byte) (err error) {
	this.outSendLock.Lock()
	defer this.outSendLock.Unlock()

	err = this.sendHeader(cHeaderCreate, tunnelId)
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

	buf_ := this.newBuf()
	defer func() {
		this.deleteBuf(buf_)
	}()
	if len(buf_) < int(length) {
		buf_ = make([]byte, length)
	}
	buf := buf_[:length]

	_, err = io.ReadFull(conn, buf)
	if err != nil {
		return
	}

	if this.creater == nil {
		err = errors.New("not slave tunnelSet!")
		return
	}

	subConn, subErr := this.creater(buf)
	if subErr != nil {
		id := tunnelId
		err1 := subErr
		go func() {
			this.sendErrorLoop(id, err1)
		}()
	} else {
		if t != nil {
			t.sendError(errors.New("tunnel overwrited!"))
		}
		t = this.createTunnel()
		this.tunnelMap.Store(tunnelId, t)
		go func() {
			//fmt.Println("slave tunnel ", tunnelId, " created!")
			defer func() {
				this.tunnelMap.Delete(tunnelId)
				//fmt.Println("slave tunnel ", tunnelId, " ended!")
			}()
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
	if sendErr == nil {
		panic("can't send nil error!")
	}
	//fmt.Println("close remote tunnel:", tunnelId)
	if sendErr.Error() != "EOF" {
		fmt.Println("last error:", sendErr)
	}
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
	closeSignal := make(chan struct{})
	defer func() {
		close(closeSignal)
		close(t.inEmptyBuf)
		conn.Close()
	}()

	go func() {
		var err error
		p := pack{
			data:    make([]byte, 4096),
			readNum: 0,
		}
		var lastState time.Time
		send := false
		for {
			select {
			case <-closeSignal:
				return
			case now := <-t.heartbeat:
				if send && now.After(lastState.Add(time.Second*10)) {
					err1 := this.sendStat(tunnelId, p.readNum)
					if err1 != nil {
						fmt.Println(err1)
					}
					fmt.Println(
						"tunnel:", tunnelId,
						"state send again, timeout:", lastState.Format("15:04:05"),
						"now:", now.Format("15:04:05"),
						"readNum:", p.readNum)
				}
			case readNum := <-t.readSignal:
				if p.readNum != readNum {
					if readNum-p.readNum != 1 {
						fmt.Println("read number not consistent!")
					}
					length, err1 := conn.Read(p.data)
					err = err1
					p.readNum = readNum
					p.size = uint32(length)
					send = false
				} else {
					fmt.Println(
						"tunnel:", tunnelId,
						"resend pack:", readNum)
				}

				if p.size != 0 {
					this.sendDataLoop(tunnelId, p)
					send = true
					lastState = time.Now().Add(time.Second * 10)
					if int(p.size) == len(p.data) && p.size < MaxDataSize {
						ndata := make([]byte, p.size*2)
						for i, c := range p.data {
							ndata[i] = c
						}
						p.data = ndata
					}
				}

				if err != nil {
					if strings.Index(err.Error(), "use of closed network") >= 0 {
						err = io.EOF
					}
					if err.Error() != "EOF" {
						fmt.Println(err)
					}
					this.sendErrorLoop(tunnelId, err)
					select {
					case t.inError_ <- err:
					default:
					}
					return
				}
			}
		}
	}()

	readNum := uint32(1)
	var lastRead time.Time
	sendRead := func(num uint32, now time.Time) {
		this.sendReadLoop(tunnelId, num)
		lastRead = now
	}
	sendRead(readNum, time.Now())
	for {
		flush := func() {
			select {
			case p := <-t.inFullBuf:
				defer func() {
					t.inEmptyBuf <- p
				}()
				conn.Write(p.data)
			default:
			}
		}
		select {
		case err = <-t.inError_:
			flush()
			return
		case p := <-t.inFullBuf:
			func() {
				defer func() {
					t.inEmptyBuf <- p
				}()
				if p.size == 0 {
					fmt.Println("closed pack!")
					return
				}
				if p.readNum == readNum {
					_, err = conn.Write(p.data[:p.size])
					if err != nil {
						this.sendErrorLoop(tunnelId, err)
						return
					}
					readNum++
					sendRead(readNum, time.Now())
					fmt.Println(
						"tunnel:", tunnelId,
						"send read number:", readNum)
				} else {
					fmt.Println(
						"tunnel:", tunnelId,
						"unknown read number:", p.readNum,
						"expect:", readNum,
						"tunnel:", tunnelId)
				}
			}()
		case ir := <-t.statSignal:
			now := time.Now()
			timeout := lastRead.Add(time.Second * 10).Before(now)
			if ir == readNum {
				if timeout {
					fmt.Println(
						"tunnel:", tunnelId,
						"recive again, lastRead:", lastRead.Format("15:04:05"),
						"now:", now.Format("15:04:05"),
						"read id:", readNum)
				} else {
					fmt.Println(
						"tunnel:", tunnelId,
						"recive lag:", now.Sub(lastRead),
						"read id:", readNum)
				}
			} else {
				if timeout {
					fmt.Println(
						"tunnel:", tunnelId,
						"read send again, lastRead:", lastRead.Format("15:04:05"),
						"now:", now.Format("15:04:05"),
						"lag id:", ir,
						"expect id:", readNum)

				} else {
					fmt.Println(
						"tunnel:", tunnelId,
						"read lag:", now.Sub(lastRead),
						"lag id:", ir,
						"expect id:", readNum)
				}
			}
			if timeout {
				sendRead(readNum, now)
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

	var header uint32
	var tunnelId uint64
	for {
		err = binary.Read(conn, binary.LittleEndian, &header)
		if err != nil {
			return
		}

		err = binary.Read(conn, binary.LittleEndian, &tunnelId)
		if err != nil {
			return
		}

		var t *tunnel
		ti, ok := this.tunnelMap.Load(tunnelId)
		if ok {
			t = ti.(*tunnel)
		}

		//fmt.Println("recive tunnel:", tunnelId, "header:", header)
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
			err = errors.New(fmt.Sprint("unknown header:", header))
		}

		//fmt.Println("recive end!")
		if err != nil {
			if err == errNotFound {
				id := tunnelId
				err1 := err
				go func() {
					this.sendErrorLoop(id, err1)
				}()
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
	//fmt.Println("tunnel ", tunnelId, " created!")
	defer func() {
		this.tunnelMap.Delete(tunnelId)
		//fmt.Println("tunnel ", tunnelId, " ended!")
	}()

	this.sendCreateLoop(tunnelId, cmd)

	err = this.runTunnel(conn, tunnelId, t)
	if err != nil && err.Error() == "EOF" {
		err = nil
	}
	return
}
