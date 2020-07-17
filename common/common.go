package common

import (
	"encoding/binary"
	"errors"
	"io"
)

var (
	maxSize      = uint32(10000000)
	replyMaxSize = uint32(4096)

	headerPack      = "pack"
	headerHeartbeat = "htbt"
	headerSuccess   = "succ"
	headerError     = "erro"

	CErrClosed = "tunnel closed!!!"
)

func getReply(conn io.Reader) (err error) {

	header := make([]byte, 4)
	_, err = io.ReadFull(conn, header)
	if err != nil {
		return
	}

	if string(header) == headerSuccess {
		return
	}

	if string(header) != headerError {
		err = errors.New("wrong reply")
	}

	var length uint32
	err = binary.Read(conn, binary.LittleEndian, &length)
	if err != nil {
		return
	}

	if length > replyMaxSize {
		err = errors.New("reply too long!")
		return
	}

	buf := make([]byte, length)
	_, err = io.ReadFull(conn, buf)
	if err != nil {
		return
	}

	err = errors.New(string(buf))
	return
}

func PutReply(conn io.Writer, result error) (err error) {
	if result == nil {
		_, err = conn.Write([]byte(headerSuccess))
		return
	}

	_, err = conn.Write([]byte(headerError))
	if err != nil {
		return
	}

	buf := []byte(result.Error())
	if len(buf) > int(replyMaxSize) {
		buf = buf[:replyMaxSize]
	}

	err = binary.Write(conn, binary.LittleEndian, uint32(len(buf)))
	if err != nil {
		return
	}
	_, err = conn.Write(buf)
	return
}

func GetPack(conn io.ReadWriter) (tunnel uint64, content []byte, err error) {

	header := make([]byte, 4)
	_, err = io.ReadFull(conn, header)
	if err != nil {
		return
	}

	if string(header) == headerHeartbeat {
		return
	}

	if string(header) != headerPack {
		err = errors.New("wrong pack header!")
		return
	}

	err = binary.Read(conn, binary.LittleEndian, &tunnel)
	if err != nil {
		return
	}

	var length uint32
	err = binary.Read(conn, binary.LittleEndian, &length)
	if err != nil {
		return
	}

	if length > maxSize {
		err = errors.New("pack too large!")
		return
	}

	buf := make([]byte, length)
	_, err = io.ReadFull(conn, buf)
	return
}

func PutPack(conn io.ReadWriter, tunnel uint64, content []byte) (err error) {
	if len(content) == 0 {
		_, err = conn.Write([]byte(headerHeartbeat))
		if err != nil {
			return
		}

		err = getReply(conn)
		return
	}
	if len(content) > int(maxSize) {
		err = errors.New("content too long!")
		return
	}

	_, err = conn.Write([]byte(headerPack))
	if err != nil {
		return
	}

	err = binary.Write(conn, binary.LittleEndian, tunnel)
	if err != nil {
		return
	}

	err = binary.Write(conn, binary.LittleEndian, uint32(len(content)))
	if err != nil {
		return
	}

	_, err = conn.Write(content)
	if err != nil {
		return
	}

	err = getReply(conn)
	return
}
