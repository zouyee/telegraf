package metric

import (
	"io"
	"io/ioutil"
	"testing"
	"time"

	"github.com/influxdata/telegraf"

	"github.com/stretchr/testify/assert"
)

func BenchmarkMetricReader(b *testing.B) {
	metrics := make([]telegraf.Metric, 10)
	for i := 0; i < 10; i++ {
		metrics[i], _ = New("foo", map[string]string{},
			map[string]interface{}{"value": int64(1)}, time.Now())
	}
	for n := 0; n < b.N; n++ {
		r := NewReader(metrics)
		io.Copy(ioutil.Discard, r)
	}
}

func TestMetricReader(t *testing.T) {
	ts := time.Unix(1481032190, 0)
	metrics := make([]telegraf.Metric, 10)
	for i := 0; i < 10; i++ {
		metrics[i], _ = New("foo", map[string]string{},
			map[string]interface{}{"value": int64(1)}, ts)
	}

	r := NewReader(metrics)

	buf := make([]byte, 35)
	for i := 0; i < 10; i++ {
		n, err := r.Read(buf)
		if err != nil {
			assert.True(t, err == io.EOF, err.Error())
		}
		assert.Equal(t, 33, n)
		assert.Equal(t, "foo value=1i 1481032190000000000\n", string(buf[0:n]))
	}

	// reader should now be done, and always return 0, io.EOF
	for i := 0; i < 10; i++ {
		n, err := r.Read(buf)
		assert.True(t, err == io.EOF, err.Error())
		assert.Equal(t, 0, n)
	}
}

func TestMetricReader_SplitMetric(t *testing.T) {
	ts := time.Unix(1481032190, 0)
	m, _ := New("foo", map[string]string{},
		map[string]interface{}{"value": int64(10)}, ts)
	metrics := []telegraf.Metric{m}

	r := NewReader(metrics)
	buf := make([]byte, 5)

	tests := []struct {
		exp string
		err error
		n   int
	}{
		{
			"foo v",
			nil,
			5,
		},
		{
			"alue=",
			nil,
			5,
		},
		{
			"10i 1",
			nil,
			5,
		},
		{
			"48103",
			nil,
			5,
		},
		{
			"21900",
			nil,
			5,
		},
		{
			"00000",
			nil,
			5,
		},
		{
			"000\n",
			io.EOF,
			4,
		},
		{
			"",
			io.EOF,
			0,
		},
	}

	for _, test := range tests {
		n, err := r.Read(buf)
		assert.Equal(t, test.n, n)
		assert.Equal(t, test.exp, string(buf[0:n]))
		assert.Equal(t, test.err, err)
	}
}

func TestMetricReader_SplitMultipleMetrics(t *testing.T) {
	ts := time.Unix(1481032190, 0)
	m, _ := New("foo", map[string]string{},
		map[string]interface{}{"value": int64(10)}, ts)
	metrics := []telegraf.Metric{m, m.Copy()}

	r := NewReader(metrics)
	buf := make([]byte, 10)

	tests := []struct {
		exp string
		err error
		n   int
	}{
		{
			"foo value=",
			nil,
			10,
		},
		{
			"10i 148103",
			nil,
			10,
		},
		{
			"2190000000",
			nil,
			10,
		},
		{
			"000\n",
			nil,
			4,
		},
		{
			"foo value=",
			nil,
			10,
		},
		{
			"10i 148103",
			nil,
			10,
		},
		{
			"2190000000",
			nil,
			10,
		},
		{
			"000\n",
			io.EOF,
			4,
		},
		{
			"",
			io.EOF,
			0,
		},
	}

	for _, test := range tests {
		n, err := r.Read(buf)
		assert.Equal(t, test.n, n)
		assert.Equal(t, test.exp, string(buf[0:n]))
		assert.Equal(t, test.err, err)
	}
}
