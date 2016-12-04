package influxdb

import (
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/valyala/fasthttp"
)

const (
	DefaultDatabase        = "stress"
	DefaultRetentionPolicy = "autogen"
)

type ClientConfig struct {
	BaseURL   string
	UserAgent string
	Timeout   time.Duration
	Username  string
	Password  string

	// Default write params
	Params WriteParams

	Gzip bool
}

// TLSConfig: tlsCfg,

type WriteParams struct {
	Database        string
	RetentionPolicy string
	Precision       string
	Consistency     string
}

type Client interface {
	Create(string) error

	Write(b []byte) (int, error)
	WriteStream(b io.Reader, size int) (int, error)
	//WriteWithParams(b []byte, params WriteParams) (int, error)

	Close() error
}

type httpClient struct {
	url []byte

	cfg ClientConfig
}

func NewClient(cfg ClientConfig) Client {
	return &httpClient{
		url: []byte(writeURLFromConfig(cfg.BaseURL, cfg.Params)),
		cfg: cfg,
	}
}

func (c *httpClient) Create(command string) error {
	if command == "" {
		command = "CREATE DATABASE " + c.cfg.Params.Database
	}

	vals := url.Values{}
	vals.Set("q", command)
	resp, err := http.PostForm(c.cfg.BaseURL+"/query", vals)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf(
			"Bad status code during Create(%s): %d, body: %s",
			command, resp.StatusCode, string(body),
		)
	}

	return nil
}

func (c *httpClient) Write(b []byte) (int, error) {
	req := c.makeRequest()
	req.Header.SetContentLength(len(b))
	req.SetBody(b)

	resp := fasthttp.AcquireResponse()

	err := fasthttp.DoTimeout(req, resp, c.cfg.Timeout)
	code := resp.StatusCode()
	if code != 204 && err == nil {
		err = fmt.Errorf("Received bad status code [%d], expected [204]", code)
	}

	fasthttp.ReleaseResponse(resp)
	fasthttp.ReleaseRequest(req)

	if err == nil {
		return len(b), nil
	}
	return 0, err
}

func (c *httpClient) WriteStream(b io.Reader, size int) (int, error) {
	req := c.makeRequest()
	req.Header.SetContentLength(size)
	req.SetBodyStream(b, size)

	resp := fasthttp.AcquireResponse()

	err := fasthttp.DoTimeout(req, resp, c.cfg.Timeout)
	code := resp.StatusCode()
	if code != 204 && err == nil {
		err = fmt.Errorf("Received bad status code [%d], expected [204]", code)
	}

	fasthttp.ReleaseResponse(resp)
	fasthttp.ReleaseRequest(req)

	if err == nil {
		return size, nil
	}
	return 0, err
}

func (c *httpClient) makeRequest() *fasthttp.Request {
	req := fasthttp.AcquireRequest()
	req.Header.SetContentTypeBytes([]byte("text/plain"))
	req.Header.SetMethodBytes([]byte("POST"))
	req.Header.SetRequestURIBytes(c.url)
	if c.cfg.Gzip {
		req.Header.SetBytesKV([]byte("Content-Encoding"), []byte("gzip"))
	}
	req.Header.SetUserAgent(c.cfg.UserAgent)
	if c.cfg.Username != "" && c.cfg.Password != "" {
		req.Header.Set("Authorization", "Basic "+basicAuth(c.cfg.Username, c.cfg.Password))
	}
	return req
}

func (c *httpClient) Close() error {
	// Nothing to do.
	return nil
}

func writeURLFromConfig(baseURL string, wp WriteParams) string {
	params := url.Values{}
	params.Set("db", wp.Database)
	if wp.RetentionPolicy != "" {
		params.Set("rp", wp.RetentionPolicy)
	}
	if wp.Precision != "n" && wp.Precision != "" {
		params.Set("precision", wp.Precision)
	}
	if wp.Consistency != "one" && wp.Consistency != "" {
		params.Set("consistency", wp.Consistency)
	}

	return baseURL + "/write?" + params.Encode()
}

// See 2 (end of page 4) http://www.ietf.org/rfc/rfc2617.txt
// "To receive authorization, the httpClient sends the userid and password,
// separated by a single colon (":") character, within a base64
// encoded string in the credentials."
// It is not meant to be urlencoded.
func basicAuth(username, password string) string {
	auth := username + ":" + password
	return base64.StdEncoding.EncodeToString([]byte(auth))
}
