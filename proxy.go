package nsq

import (
	"io/ioutil"
	"net/http"
)

// ProxyPublish constructs http.Handler that forwards its POST content to
// specified topic
func (q *Queue) ProxyPublish(topic string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		if err = q.Producer.Publish(topic, data); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
	}
}
