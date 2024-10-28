package nsq

import (
	"fmt"
	"strings"
	"sync/atomic"

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

func (q *RRProducer) Publish(topic string, payload []byte) error {
	for i := range q.producers {
		idx := atomic.AddUint64(&q.idx, 1) - 1
		p := q.producers[idx%uint64(len(q.producers))]
		if err := p.Publish(topic, payload); err != nil {
			if i == len(q.producers)-1 {
				return fmt.Errorf("publishing to NSQd %d: %+v", idx, err)
			}
			continue
		}
		return nil
	}
	return fmt.Errorf("no producers configured")
}

func (q *RRProducer) Ping() error {
	for _, p := range q.producers {
		if err := p.Ping(); err != nil {
			return err
		}
	}
	return nil
}
