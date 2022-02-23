package dbhelper

import (
	"fmt"
	"time"
)

//datetime.go help format the time.Time to SQL string

func _timeToSQLTimeStr(t time.Time) string {
	return fmt.Sprintf(`%04d-%02d-%02d %02d:%02d:%02d`, t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second())
}
func timeToSQLTimeStr(t time.Time) string {
	return t.Format(`2006-01-02 15:04:05`)
}
func timeToSQLTimeStrMillisecond(t time.Time) string {
	// return t.Format(`2006-01-02 15:04:05.999`) // 2020-10-22 14:30:29.25 without trailing zeros
	return t.Format(`2006-01-02 15:04:05.000`) //2020-10-22 14:30:29.250
}

func ToSQLDatetimeStringLocal(t time.Time) string {
	return timeToSQLTimeStr(t)
}
func ToSQLDatetimeStringLocalMilliSecond(t time.Time) string {
	return timeToSQLTimeStrMillisecond(t)
}
