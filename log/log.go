package log

import "github.com/ccding/go-logging/logging"

var log *logging.Logger

func init() {
	var err error

	if log, err = logging.SimpleLogger("goa"); err != nil {
		panic(err)
	}
}

func EnableDebug() {
	log.SetLevel(logging.DEBUG)
}

func Debug(v ...interface{}) {
	log.Debug(v...)
}

func Flush() {
	log.Flush()
}
