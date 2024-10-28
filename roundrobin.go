package nsq

import (
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nsqio/go-nsq"
)

type RRProducer struct {
	producers []*nsq.Producer
	idx       uint64
}

type NsqLogger interface {
	Output(calldepth int, s string) error
}

func (q *RRProducer) SetLogger(logger NsqLogger, level nsq.LogLevel) {
	for _, pr := range q.producers {
		pr.SetLogger(logger, level)
	}
}

var _ Publisher = (*RRProducer)(nil)

func NewRRProducer(options string, nsqDs []string, logger NsqLogger, logLevel nsq.LogLevel) (*RRProducer, error) {
	cfg := nsq.NewConfig()
	for _, opt := range strings.Split(options, ",") {
		vals := strings.SplitN(strings.TrimSpace(opt), "=", 2)
		var val interface{}
		if len(vals) == 1 {
			val = true
		} else {
			val = vals[1]
		}
		if err := cfg.Set(vals[0], val); err != nil {
			return nil, fmt.Errorf("setting NSQ option: %+v", err)
		}
	}
	res := new(RRProducer)
	for _, uri := range nsqDs {
		pr, err := nsq.NewProducer(uri, cfg)
		if err != nil {
			return nil, fmt.Errorf("creating producer for %s: %+v", uri, err)
		}
		pr.SetLogger(logger, logLevel)
		res.producers = append(res.producers, pr)
	}
	return res, nil
}

func (q *RRProducer) roundRobin(f func(p Publisher) error) error {
	for i := range q.producers {
		idx := atomic.AddUint64(&q.idx, 1) - 1
		p := q.producers[idx%uint64(len(q.producers))]
		if err := f(p); err != nil {
			if i == len(q.producers)-1 {
				return fmt.Errorf("publishing to NSQd %d: %+v", idx, err)
			}
			continue
		}
		return nil
	}
	return fmt.Errorf("no producers configured")
}

// DeferredPublish implements Publisher.
func (q *RRProducer) DeferredPublish(topic string, delay time.Duration, body []byte) error {
	return q.roundRobin(func(p Publisher) error {
		return p.DeferredPublish(topic, delay, body)
	})
}

// DeferredPublishAsync implements Publisher.
func (q *RRProducer) DeferredPublishAsync(topic string, delay time.Duration, body []byte, doneChan chan *nsq.ProducerTransaction, args ...interface{}) error {
	return q.roundRobin(func(p Publisher) error {
		return p.DeferredPublishAsync(topic, delay, body, doneChan, args...)
	})
}

// MultiPublish implements Publisher.
func (q *RRProducer) MultiPublish(topic string, body [][]byte) error {
	return q.roundRobin(func(p Publisher) error {
		return p.MultiPublish(topic, body)
	})
}

// MultiPublishAsync implements Publisher.
func (q *RRProducer) MultiPublishAsync(topic string, body [][]byte, doneChan chan *nsq.ProducerTransaction, args ...interface{}) error {
	return q.roundRobin(func(p Publisher) error {
		return p.MultiPublishAsync(topic, body, doneChan, args...)
	})
}

// Publish implements Publisher.
func (q *RRProducer) Publish(topic string, body []byte) error {
	return q.roundRobin(func(p Publisher) error {
		return p.Publish(topic, body)
	})
}

// PublishAsync implements Publisher.
func (q *RRProducer) PublishAsync(topic string, body []byte, doneChan chan *nsq.ProducerTransaction, args ...interface{}) error {
	return q.roundRobin(func(p Publisher) error {
		return p.PublishAsync(topic, body, doneChan, args...)
	})
}

func (q *RRProducer) Ping() error {
	for _, p := range q.producers {
		if err := p.Ping(); err != nil {
			return err
		}
	}
	return nil
}
