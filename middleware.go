package nsq

import (
	"github.com/nsqio/go-nsq"
)

// Middleware is a handler modifier function
type Middleware func(nsq.HandlerFunc) nsq.HandlerFunc

// MiddlewareChain is a sequentially applied Middleware
type MiddlewareChain []Middleware

// Apply creates new HandlerFunc by applying all middlewares in a list
func (mc MiddlewareChain) Apply(h nsq.HandlerFunc) (res nsq.HandlerFunc) {
	res = h
	for i := len(mc) - 1; i >= 0; i-- {
		res = mc[i](res)
	}
	return
}

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
