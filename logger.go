package nsq

import (
	"strings"

	nsq "github.com/nsqio/go-nsq"
)

// Logger provides basic log output functions for all levels used in nsq
type Logger interface {
	Debug(...interface{})
	Info(...interface{})
	Warn(...interface{})
	Error(...interface{})
}

// Log levels from go-nsq package to reduce import footprint
const (
	ErrorLevel nsq.LogLevel = nsq.LogLevelError
	WarnLevel  nsq.LogLevel = nsq.LogLevelWarning
	InfoLevel  nsq.LogLevel = nsq.LogLevelInfo
	DebugLevel nsq.LogLevel = nsq.LogLevelDebug
)

// Output implements stdlib log.Logger.Output using the underlying logger
func (q *Queue) Output(_ int, s string) error {
	if q.Logger == nil {
		return nil
	}
	s = strings.TrimSpace(s)
	var level string
	if len(s) >= 3 {
		level, s = s[:3], s[3:]
	}
	switch level {
	case ErrorLevel.String():
		if !q.KeepNsqLookupD404 && strings.Contains(s, "TOPIC_NOT_FOUND") {
			return nil
		}
		q.Logger.Error(s)
	case WarnLevel.String():
		q.Logger.Warn(s)
	case DebugLevel.String():
		q.Logger.Debug(s)
	default:
		q.Logger.Info(s)
	}
	return nil
}

// SetLogger changes Logger and LogLevel for Producer and all of the Consumers
func (q *Queue) SetLogger(logger Logger, level nsq.LogLevel) {
	q.Logger = logger
	q.LogLevel = level
	q.Producer.SetLogger(q, q.LogLevel)
	for i := range q.consumers {
		q.consumers[i].SetLogger(q, q.LogLevel)
	}
}
