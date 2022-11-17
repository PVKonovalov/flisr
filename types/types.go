package types

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type IsoDate struct {
	time.Time
}

func (c *IsoDate) UnmarshalJSON(b []byte) (err error) {
	s := strings.Trim(string(b), `"`) // remove quotes
	c.Time, err = time.Parse("2006-01-02T15:04:05.999-0700", s)
	if err != nil {
		c.Time = time.Now()
	}
	return
}

func (c *IsoDate) MarshalJSON() ([]byte, error) {
	str := c.Time.Format("2006-01-02T15:04:05.999-0700")
	return []byte("\"" + str + "\""), nil
}

func (c IsoDate) String() string {
	return c.Time.Format("02.01.2006 15:04:05.999-0700")
}

type RtdbMessage struct {
	Timestamp     IsoDate `json:"ts"`
	TimestampRecv IsoDate `json:"tsr"`
	Id            uint64  `json:"id"`
	Value         float32 `json:"v"`
	Quality       uint32  `json:"qds"`
	Source        uint32  `json:"src"`
	Select        int     `json:"s"`
	Execute       int     `json:"e"`
}

func (c RtdbMessage) String() string {
	return fmt.Sprintf("[%d V: %.4f T: %s Q: %d S: %d]", c.Id, c.Value, c.Timestamp, c.Quality, c.Source)
}

// ParseScadaRtdbData Parse data from Scada RTDB to Go internal types
func ParseScadaRtdbData(data []byte) ([]RtdbMessage, error) {
	var rtdbMessage []RtdbMessage
	err := json.Unmarshal(data, &rtdbMessage)
	return rtdbMessage, err
}
