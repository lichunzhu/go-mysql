package client

import (
	"encoding/binary"
	. "github.com/lichunzhu/go-mysql/mysql"
	"github.com/pingcap/errors"
)

type Rows struct {
	*Conn
	*Result
	binary bool
	err error
}

func (c *Conn) Query(command string, args ...interface{}) (*Rows, error) {
	if len(args) == 0 {
		return c.execRows(command)
	} else {
		if s, err := c.Prepare(command); err != nil {
			return nil, errors.Trace(err)
		} else {
			var r *Result
			r, err = s.Execute(args...)
			s.Close()
			return &Rows{
				Conn:   c,
				Result: r,
			}, err
		}
	}
}

func (c *Rows) Next() bool {
	var data []byte
	var err error
	result := c.Result

	rawPkgLen := len(result.RawPkg)
	result.RawPkg, err = c.ReadPacketReuseMem(result.RawPkg)
	if err != nil {
		c.err = err
		return false
	}
	data = result.RawPkg[rawPkgLen:]

	// EOF Packet
	if c.isEOFPacket(data) {
		if c.capability&CLIENT_PROTOCOL_41 > 0 {
			//result.Warnings = binary.LittleEndian.Uint16(data[1:])
			//todo add strict_mode, warning will be treat as error
			result.Status = binary.LittleEndian.Uint16(data[3:])
			c.status = result.Status
		}

		return false
	}

	if data[0] == ERR_HEADER {
		c.err = c.handleErrorPacket(data)
		return false
	}

	result.RowDatas[0] = data
	result.Values[0], err = result.RowDatas[0].ParsePureText(result.Fields, result.Values[0])
	if err != nil {
		c.err = errors.Trace(err)
		return false
	}

	return true
}
