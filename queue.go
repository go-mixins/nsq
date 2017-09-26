// Package nsq provides simple wrapper for NSQ Consumer and Producer, hopefully
// simplifying use of both.
//
// Some example usage:
//
// q := new(nsq.Queue)
// envconfig.Process("PREFIX", q)
// q.Init()
// _, err := q.AddConsumer("queue1", "channel", handlerFunc1, otherHandlerFunc)
// _, err = q.AddConsumer("queue2", "channel", handlerFunc2)
// ...
// err = q.Connect()
// ...
// q.Publish("queue", &obj)
//
package nsq

import (
	"github.com/nsqio/go-nsq"
	"github.com/pkg/errors"

	"encoding/json"
	"strings"
	"sync"
)

type logger interface {
	Output(calldepth int, s string) error
}

// Queue combines NSQ message producer and consumer into one object. The
// structure members specify configuration parameters and must be configured
// externally, e.g. using envconfig.
type Queue struct {
	// NSQD connection URL for producer
	NsqD string `default:"localhost:4150"`
	// NSQLookupD URL for message consumer(s)
	NsqLookupD string `default:"localhost:4161"`
	// Common NSQ options both for consumer and producer. Specified as a
	// comma-separated list of key=value pairs.
	NsqOptions string `default:"max_attempts=65535"`

	*nsq.Producer
	consumers []*nsq.Consumer
	nsqConfig *nsq.Config
	l         logger
	lvl       nsq.LogLevel
}

// Init must be called before adding message handlers and producing messages
func (q *Queue) Init() (err error) {
	q.nsqConfig = nsq.NewConfig()
	for _, opt := range strings.Split(q.NsqOptions, ",") {
		vals := strings.SplitN(strings.TrimSpace(opt), "=", 2)
		var val interface{}
		if len(vals) == 1 {
			val = true
		} else {
			val = vals[1]
		}
		if err = q.nsqConfig.Set(vals[0], val); err != nil {
			err = errors.Wrap(err, "setting NSQ option")
			return
		}
	}
	q.Producer, err = nsq.NewProducer(q.NsqD, q.nsqConfig)
	err = errors.Wrap(err, "creating producer")
	return
}

// SetLogger forwards its arguments to the corresponding method of the producer and
// all of the consumers. It can be used to connect external logging facilities.
func (q *Queue) SetLogger(l logger, lvl nsq.LogLevel) {
	q.l = l
	q.lvl = lvl
	q.Producer.SetLogger(l, lvl)
	for i := range q.consumers {
		q.consumers[i].SetLogger(l, lvl)
	}
}

// AddConsumer creates new consumer for given topic and channel and adds
// specified handlers if any
func (q *Queue) AddConsumer(topic, channel string, handlers ...nsq.HandlerFunc) (res *nsq.Consumer, err error) {
	if res, err = nsq.NewConsumer(topic, channel, q.nsqConfig); err != nil {
		err = errors.Wrapf(err, "creating consumer")
		return
	}
	q.consumers = append(q.consumers, res)
	for _, f := range handlers {
		res.AddHandler(f)
	}
	res.SetLogger(q.l, q.lvl)
	return
}

// AddConsumerN creates new consumer for given topic and channel and
// assigns handler with specified concurency
func (q *Queue) AddConsumerN(topic, channel string, concurrency int, handler nsq.HandlerFunc, mw ...Middleware) (res *nsq.Consumer, err error) {
	if res, err = nsq.NewConsumer(topic, channel, q.nsqConfig); err != nil {
		err = errors.Wrapf(err, "creating topic %q", topic)
		return
	}
	q.consumers = append(q.consumers, res)
	res.AddConcurrentHandlers(MiddlewareChain(mw).Apply(handler), concurrency)
	res.ChangeMaxInFlight(concurrency * 10)
	res.SetLogger(q.l, q.lvl)
	return
}

// Close sends a stop signal to consumers and blocks until all of them are stopped
func (q *Queue) Close() {
	var wg sync.WaitGroup
	for i := range q.consumers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			q.consumers[i].Stop()
			<-q.consumers[i].StopChan
		}(i)
	}
	wg.Wait()
}

// Connect connects consumers to the queue
func (q *Queue) Connect() (err error) {
	for i := range q.consumers {
		if q.NsqLookupD == "" {
			err = q.consumers[i].ConnectToNSQD(q.NsqD)
		} else {
			err = q.consumers[i].ConnectToNSQLookupd(q.NsqLookupD)
		}
		if err != nil {
			return errors.Wrapf(err, "connecting consumer to NSQ")
		}
	}
	return
}

// Publish marshals object to JSON and sends it to the specified topic
func (q *Queue) Publish(topic string, obj interface{}) error {
	jsonData, err := json.Marshal(obj)
	if err != nil {
		return errors.Wrapf(err, "marshaling JSON")
	}
	return errors.Wrapf(q.Producer.Publish(topic, jsonData), "publishing to NSQ")
}
