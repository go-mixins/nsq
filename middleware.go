package nsq

import (
	"github.com/nsqio/go-nsq"
)

// Recover provides simple middleware that calls specified handler in case of
// panic and returns its result as a result of handler execution
func Recover(h func(e interface{}) error) func(nsq.HandlerFunc) nsq.HandlerFunc {
	return func(f nsq.HandlerFunc) nsq.HandlerFunc {
		return func(msg *nsq.Message) (err error) {
			defer func() {
				if e := recover(); e != nil {
					err = h(e)
				}
			}()
			err = f(msg)
			return
		}
	}
}
