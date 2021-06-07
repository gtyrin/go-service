package service

import (
	"runtime/debug"

	log "github.com/sirupsen/logrus"
)

// FailOnError print out an error message into log and stop process execution.
func FailOnError(err error, context string) {
	if err != nil {
		debug.PrintStack()
		log.WithField("context", context).Fatal(err)
	}
}

// LogOnError print out an error message into log
func LogOnError(err error, context string) {
	if err != nil {
		debug.PrintStack()
		log.WithField("context", context).Error(err)
	}
}
