package client

import (
	"bytes"
	"encoding/binary"
	. "github.com/lichunzhu/go-mysql/mysql"
	"github.com/lichunzhu/go-mysql/utils"
	"github.com/pingcap/errors"
	"sync"
)

var (
	fieldValuePool = sync.Pool{
		New: func() interface{} {
			return make([]FieldValue, 0)
		},
	}
)

func FieldValueGet() (fieldValue []FieldValue) {
	return fieldValuePool.New().([]FieldValue)
}

func FieldValuePut(fieldValue []FieldValue) {
	fieldValuePool.Put(fieldValue)
}


type Rows struct {
	*Conn
	*Result
	binary             bool
	err                error
	parseErr  	       chan error
	RawBytesBufferChan chan *bytes.Buffer
	OutputValueChan    chan []FieldValue
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

func (c *Rows) Start() bool {
	var data []byte
	result := c.Result
	defer close(c.RawBytesBufferChan)
	go c.KeepParsing()
	for {
		bf, err := c.ReadPacketReuseMemNoCopy()
		if err != nil {
			c.err = err
			return false
		}
		data = bf.Bytes()

		// EOF Packet
		if c.isEOFPacket(data) {
			if c.capability&CLIENT_PROTOCOL_41 > 0 {
				//result.Warnings = binary.LittleEndian.Uint16(data[1:])
				//todo add strict_mode, warning will be treat as error
				result.Status = binary.LittleEndian.Uint16(data[3:])
				c.status = result.Status
			}

			return true
		}

		if data[0] == ERR_HEADER {
			c.err = c.handleErrorPacket(data)
			return false
		}

		c.RawBytesBufferChan <- bf

		select {
		case c.err = <-c.parseErr:
			return false
		default:
		}
	}

}

func (c *Rows) KeepParsing() {
	var (
		rowData RowData
		value []FieldValue
		err error
	)
	for data := range c.RawBytesBufferChan {
		rowData = data.Bytes()
		value = FieldValueGet()
		value, err = rowData.ParsePureText(c.Result.Fields, value)
		if err != nil {
			c.parseErr <- errors.Trace(err)
		}
		utils.BytesBufferPut(data)
		c.OutputValueChan <- value
	}
}

func (c *Rows) FinishReading(fieldValue []FieldValue) {
	FieldValuePut(fieldValue)
}
