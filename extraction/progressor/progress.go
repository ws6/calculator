package progressor

import (
	"time"
)

type Progress struct {
	Timestamp time.Time
	Number    int64
	String    string
}

func (self *Progress) Copy(dst *Progress) {
	if dst == nil {
		return
	}
	dst.Timestamp = self.Timestamp
	dst.Number = self.Number
	dst.String = self.String
}
