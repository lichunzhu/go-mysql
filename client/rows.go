package client

import (
	"bytes"
	"encoding/binary"
	"fmt"
	. "github.com/lichunzhu/go-mysql/mysql"
	"github.com/lichunzhu/go-mysql/utils"
	"github.com/pingcap/errors"
	"sync"
)

type OutputResult struct {
	RawBytesBuf    *bytes.Buffer
	FieldResultArr []FieldValue
}

var (
	outputResultPool = sync.Pool{
		New: func() interface{} {
			return &OutputResult{}
		},
	}
	outputResultChan = make(chan *OutputResult, 10)
)

func OutputResultGet() (data *OutputResult) {
	select {
	case data = <-outputResultChan:
	default:
		data = outputResultPool.New().(*OutputResult)
	}

	return data
}

func OutputResultPut(data *OutputResult) {
	select {
	case outputResultChan <- data:
	default:
		outputResultPool.Put(data)
	}
}


type Rows struct {
	*Conn
	*Result
	binary             bool
	err                error
	parseErr  	       chan error
	RawBytesBufferChan chan *bytes.Buffer
	OutputValueChan    chan *OutputResult
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

func (c *Rows) Start() error {
	var data []byte
	result := c.Result
	defer close(c.RawBytesBufferChan)
	go c.KeepParsing()
	for {
		bf, err := c.ReadPacketReuseMemNoCopy()
		if err != nil {
			c.err = err
			return err
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

			return nil
		}

		if data[0] == ERR_HEADER {
			c.err = c.handleErrorPacket(data)
			return c.err
		}

		c.RawBytesBufferChan <- bf

		select {
		case c.err = <-c.parseErr:
			return c.err
		default:
		}
	}

}

func (c *Rows) KeepParsing() {
	defer close(c.OutputValueChan)
	var (
		rowData RowData
		err error
	)
	cnt := 0
	for data := range c.RawBytesBufferChan {
		if err != nil {
			continue
		}
		rowData = data.Bytes()
		ores := OutputResultGet()
		ores.RawBytesBuf = data
		if len(ores.FieldResultArr) < len(c.Result.Fields) {
			cnt+=1
			fmt.Printf("cnt: %d, len(ores): %d, len(fields): %d\n", cnt, len(ores.FieldResultArr), len(c.Result.Fields))
			ores.FieldResultArr = make([]FieldValue, len(c.Result.Fields))
		}
		ores.FieldResultArr, err = rowData.ParsePureText(c.Result.Fields, ores.FieldResultArr)
		if err != nil {
			c.parseErr <- errors.Trace(err)
		}
		c.OutputValueChan <- ores
	}
	fmt.Printf("cnt: %d\n", cnt)
}

func (c *Rows) FinishReading(result *OutputResult) {
	utils.BytesBufferPut(result.RawBytesBuf)
	OutputResultPut(result)
}
