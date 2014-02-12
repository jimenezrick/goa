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

func Debugf(format string, v ...interface{}) {
	log.Debugf(format, v...)
}

func Info(v ...interface{}) {
	log.Info(v...)
}

func Infof(format string, v ...interface{}) {
	log.Infof(format, v...)
}

func Flush() {
	log.Flush()
}
