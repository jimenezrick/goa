package goa

import (
	"bufio"
	"encoding/binary"
	"io"
	"net"
)

type conn struct {
	c   net.Conn
	buf *bufio.ReadWriter
}

func newConn(c net.Conn) *conn {
	return &conn{
		c,
		bufio.NewReadWriter(bufio.NewReader(c), bufio.NewWriter(c)),
	}
}

func (conn *conn) sendBatch(reqs []*Request) error {
	for _, r := range reqs {
		if err := conn.send(r); err != nil {
			return err
		}
	}

	return conn.buf.Flush()
}

func (conn *conn) send(req *Request) error {
	if err := conn.writeHeader(req); err != nil {
		return err
	}

	if _, err := conn.buf.Write(req.payld); err != nil {
		return err
	}

	return nil
}

func (conn *conn) recv() ([]byte, uint64, error) {
	seq, size, err := conn.readHeader()
	if err != nil {
		return nil, 0, err
	}

	// TODO: too much allocation? memory pool? slab?
	payld := make([]byte, size)
	_, err = io.ReadFull(conn.buf, payld)
	if err != nil {
		return nil, 0, err
	}

	return payld, seq, nil
}

func (conn *conn) close() error {
	if err := conn.buf.Flush(); err != nil {
		return err
	}

	return conn.c.Close()
}

func (conn *conn) writeHeader(req *Request) error {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, req.seq)
	_, err := conn.buf.Write(buf[:n])
	if err != nil {
		return err
	}

	n = binary.PutUvarint(buf, uint64(len(req.payld)))
	_, err = conn.buf.Write(buf[:n])
	if err != nil {
		return err
	}

	return nil
}

func (conn *conn) readHeader() (uint64, uint64, error) {
	seq, err := binary.ReadUvarint(conn.buf)
	if err != nil {
		return 0, 0, err
	}

	size, err := binary.ReadUvarint(conn.buf)
	if err != nil {
		return 0, 0, err
	}

	return seq, size, nil
}
