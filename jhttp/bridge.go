// Copyright (C) 2017 Michael J. Fromberger. All Rights Reserved.

// Package jhttp implements a bridge from HTTP to JSON-RPC.  This permits
// requests to be submitted to a JSON-RPC server using HTTP as a transport.
package jhttp

import (
	"fmt"
	"io"
	"mime"
	"net/http"
	"slices"
	"strconv"

	"github.com/stellar-experimental/jrpc2"
)

// A Bridge is a [http.Handler] that bridges requests to a JSON-RPC server.
//
// By default, the bridge accepts only HTTP POST requests with the complete
// JSON-RPC request message in the body, with Content-Type application/json.
// Either a single request object or a list of request objects is supported.
//
// If the HTTP request method is not "POST", the bridge reports 405 (Method Not
// Allowed). If the Content-Type is not application/json, the bridge reports
// 415 (Unsupported Media Type).
//
// If a ParseRequest hook is set, these requirements are disabled, and the hook
// is entirely responsible for checking request structure.
//
// If a ParseGETRequest hook is set, HTTP "GET" requests are handled by a
// Getter using that hook; otherwise "GET" requests are handled as above.
//
// If the request completes, whether or not there is an error, the HTTP
// response is 200 (OK) for ordinary requests or 204 (No Response) for
// notifications, and the response body contains the JSON-RPC response.
type Bridge struct {
	srv      *jrpc2.Server
	parseReq func(*http.Request) ([]*jrpc2.ParsedRequest, error)
	getter   *Getter
}

// ServeHTTP implements the required method of [http.Handler].
func (b Bridge) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// If a GET hook is defined, allow GET requests.
	if req.Method == "GET" && b.getter != nil {
		b.getter.ServeHTTP(w, req)
		return
	}

	// If no parse hook is defined, insist that the method is POST and the
	// content-type is application/json. Setting a hook disables these checks.
	if b.parseReq == nil {
		// Advertise that we accept POST application/json.
		w.Header().Set("Accept-Post", "application/json")

		if req.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		mt, params, _ := mime.ParseMediaType(req.Header.Get("Content-Type"))
		if mt != "application/json" {
			http.Error(w, "content-type must be application/json", http.StatusUnsupportedMediaType)
			return
		} else if cs, ok := params["charset"]; ok && cs != "utf-8" && cs != "utf8" {
			http.Error(w, "invalid content-type charset", http.StatusUnsupportedMediaType)
			return
		}
	}
	if err := b.serveInternal(w, req); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintln(w, err.Error())
	}
}

func (b Bridge) serveInternal(w http.ResponseWriter, req *http.Request) error {
	jreq, err := b.parseHTTPRequest(req)
	if err != nil {
		return err
	}

	// Statically invalid requests are answered ahead of the rest.
	slices.SortStableFunc(jreq, func(a, b *jrpc2.ParsedRequest) int {
		switch {
		case a.Error != nil && b.Error == nil:
			return -1
		case a.Error == nil && b.Error != nil:
			return 1
		}
		return 0
	})

	rsps := b.srv.ServeRequests(req.Context(), jreq)
	if len(rsps) == 0 {
		w.WriteHeader(http.StatusNoContent) // only notifications, or an empty batch
		return nil
	}
	return writeResponses(w, jreq[0].Batch || len(rsps) > 1, rsps)
}

func (b Bridge) parseHTTPRequest(req *http.Request) ([]*jrpc2.ParsedRequest, error) {
	if b.parseReq != nil {
		return b.parseReq(req)
	}
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	return jrpc2.ParseRequests(body)
}

// writeResponses writes rsps as the body of a 200 response, as an array if
// batch is true. Results are written as-is, without re-encoding.
func writeResponses(w http.ResponseWriter, batch bool, rsps []*jrpc2.Response) error {
	n, err := writeBody(io.Discard, batch, rsps) // check the encoding and measure it
	if err != nil {
		return err
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.FormatInt(n, 10))
	w.WriteHeader(http.StatusOK)
	writeBody(w, batch, rsps) // a write error means the client is gone
	return nil
}

var (
	openBracket  = []byte("[")
	closeBracket = []byte("]")
	comma        = []byte(",")
)

func writeBody(w io.Writer, batch bool, rsps []*jrpc2.Response) (int64, error) {
	sw := &stickyWriter{w: w}
	if batch {
		sw.Write(openBracket)
	}
	for i, rsp := range rsps {
		if i > 0 {
			sw.Write(comma)
		}
		if _, err := rsp.WriteTo(sw); err != nil {
			return sw.n, err
		}
	}
	if batch {
		sw.Write(closeBracket)
	}
	return sw.n, sw.err
}

// A stickyWriter counts the bytes written to w and stops at the first error.
type stickyWriter struct {
	w   io.Writer
	n   int64
	err error
}

func (s *stickyWriter) Write(p []byte) (int, error) {
	if s.err != nil {
		return 0, s.err
	}
	n, err := s.w.Write(p)
	s.n += int64(n)
	s.err = err
	return n, err
}

// Close is a no-op retained for compatibility; a Bridge holds no resources.
func (b Bridge) Close() error { return nil }

// NewBridge constructs a new Bridge that dispatches HTTP requests to a server
// on mux. Handlers run on the goroutine and context of their HTTP request.
// The server cannot push calls or notifications to the remote client.
func NewBridge(mux jrpc2.Assigner, opts *BridgeOptions) Bridge {
	b := Bridge{
		srv:      jrpc2.NewServer(mux, opts.serverOptions()),
		parseReq: opts.parseRequest(),
	}
	if pget := opts.parseGETRequest(); pget != nil {
		b.getter = &Getter{srv: b.srv, parseReq: pget}
	}
	return b
}

// BridgeOptions are optional settings for a Bridge. A nil pointer is ready for
// use and provides default values as described.
type BridgeOptions struct {
	// Deprecated: The bridge no longer uses a client. This field is ignored.
	Client *jrpc2.ClientOptions

	// Options for the bridge server (default nil).
	Server *jrpc2.ServerOptions

	// If non-nil, this function is called to parse JSON-RPC requests from the
	// HTTP request body. If this function reports an error, the request fails.
	// By default, the bridge uses jrpc2.ParseRequests on the HTTP request body.
	//
	// Setting this hook disables the default requirement that the request
	// method be POST and the content-type be application/json.
	ParseRequest func(*http.Request) ([]*jrpc2.ParsedRequest, error)

	// If non-nil, this function is used to parse a JSON-RPC method name and
	// parameters from the URL of an HTTP GET request. If this function reports
	// an error, the request fails.
	//
	// If this hook is set, all GET requests are handled by a Getter using this
	// parse function, and are not passed to a ParseRequest hook even if one is
	// defined.
	ParseGETRequest func(*http.Request) (string, any, error)
}

func (o *BridgeOptions) serverOptions() *jrpc2.ServerOptions {
	if o == nil {
		return nil
	}
	return o.Server
}

func (o *BridgeOptions) parseRequest() func(*http.Request) ([]*jrpc2.ParsedRequest, error) {
	if o == nil {
		return nil
	}
	return o.ParseRequest
}

func (o *BridgeOptions) parseGETRequest() func(*http.Request) (string, any, error) {
	if o == nil {
		return nil
	}
	return o.ParseGETRequest
}
