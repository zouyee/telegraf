package metric

import (
	"io"

	"github.com/influxdata/telegraf"
)

type state int

const (
	_ state = iota
	// normal state copies whole metrics into the given buffer until we can't
	// fit the next metric.
	normal
	// split state means that we have a metric that didn't fit into a single
	// buffer, and needs to be split across multiple calls to Read.
	split
	// done means we're done reading metrics, and now always return (0, io.EOF)
	done
)

type reader struct {
	metrics []telegraf.Metric
	buf     []byte
	state   state

	// metric index
	iM int
	// buffer index
	iB int
}

func NewReader(metrics []telegraf.Metric) io.Reader {
	return &reader{
		metrics: metrics,
		state:   normal,
	}
}

func (r *reader) Read(p []byte) (n int, err error) {
	if r.state == done {
		return 0, io.EOF
	}
	i := 0

	switch r.state {
	case normal:
		for {
			if r.metrics[r.iM].Len() < len(p[i:]) {
				r.metrics[r.iM].CopyTo(p[i:])
			} else {
				break
			}
			i += r.metrics[r.iM].Len()
			r.iM++
			if r.iM == len(r.metrics) {
				r.state = done
				return i, io.EOF
			}
		}

		// if we haven't written any bytes and we're not at the end of the metrics
		// slice, then it means we have a single metric that is larger than the
		// provided buffer.
		if i == 0 {
			r.buf = r.metrics[r.iM].Serialize()
			i += copy(p, r.buf[r.iB:])
			r.iB += i
			r.state = split
		}

	case split:
		i = copy(p, r.buf[r.iB:])
		r.iB += i
		if r.iB >= len(r.buf) {
			r.iB = 0
			r.iM++
			if r.iM == len(r.metrics) {
				r.state = done
				return i, io.EOF
			}
			r.state = normal
		}
	}

	return i, nil
}
