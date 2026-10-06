package jrpc2

import (
	"context"
	"encoding/json"
	"io"
	"sync"
	"time"
)

// ServeRequests dispatches reqs to their handlers without a channel and
// returns the responses in request order, omitting notifications unless their
// Error field is set. The server need not be started.
//
// Handler contexts derive from ctx; ServerOptions.NewContext is not used.
// Requests run concurrently under the server's concurrency limit, the last on
// the calling goroutine. Unlike a started server, ServeRequests does not check
// for duplicate IDs, support [Server.CancelRequest], or count bytes in
// [ServerMetrics].
//
// A non-empty [json.RawMessage] result is sent verbatim, without validation;
// the handler must not modify it after returning.
func (s *Server) ServeRequests(ctx context.Context, reqs []*ParsedRequest) []*Response {
	start := time.Now()
	rpcRequestsCount.Add(int64(len(reqs)))

	ts := make(tasks, len(reqs))
	s.mu.Lock()
	for i, req := range reqs {
		t := &task{hreq: &Request{method: req.Method, params: req.Params}}
		if req.ID != "" {
			t.hreq.id = fixID(json.RawMessage(req.ID))
		}
		t.ctx = context.WithValue(ctx, inboundRequestKey{}, t.hreq)
		if req.Error != nil {
			t.err = req.Error
		} else if req.Method == "" {
			t.err = errEmptyMethod
		} else if t.m = s.assignLocked(t.ctx, req.Method); t.m == nil {
			t.err = errNoSuchMethod.WithData(req.Method)
		}
		if t.err != nil {
			s.log("Request check error for %q (params %q): %v",
				req.Method, string(req.Params), t.err)
			rpcErrorsCount.Add(1)
		}
		ts[i] = t
	}
	s.mu.Unlock()

	todo, _ := ts.numToDo()
	var wg sync.WaitGroup
	for _, t := range ts {
		if t.err != nil {
			continue
		}
		todo--
		if todo == 0 {
			t.val, t.err = s.invokeRaw(t.ctx, t.m, t.hreq)
			break
		}
		wg.Go(func() { t.val, t.err = s.invokeRaw(t.ctx, t.m, t.hreq) })
	}
	wg.Wait()

	var rsps []*Response
	for i, t := range ts {
		if t.hreq.id == nil && reqs[i].Error == nil {
			continue // a notification
		}
		rsp := &Response{id: "null", result: t.val}
		if t.hreq.id != nil {
			rsp.id = string(t.hreq.id)
		}
		if e, ok := t.err.(*Error); ok {
			rsp.err = e
		} else if c := ErrorCode(t.err); c != NoError {
			rsp.err = &Error{Code: c, Message: t.err.Error()}
		} else if t.err != nil {
			rsp.err = &Error{Code: InternalError, Message: t.err.Error()}
		}
		s.rpcLog.LogResponse(t.ctx, rsp)
		rsps = append(rsps, rsp)
	}
	s.log("Completed %d requests [%v elapsed]", len(reqs), time.Since(start))
	return rsps
}

// invokeRaw is like invoke, but returns a non-empty json.RawMessage result
// as-is, without validation or escaping.
func (s *Server) invokeRaw(base context.Context, h Handler, req *Request) (json.RawMessage, error) {
	ctx := context.WithValue(base, serverKey{}, s)
	if err := s.sem.Acquire(ctx, 1); err != nil {
		return nil, err
	}
	defer s.sem.Release(1)

	s.rpcLog.LogRequest(ctx, req)
	v, err := h(ctx, req)
	if err != nil {
		if req.IsNotification() {
			s.log("Discarding error from notification to %q: %v", req.Method(), err)
			return nil, nil // a notification
		}
		return nil, err // a call reporting an error
	}
	if raw, ok := v.(json.RawMessage); ok && len(raw) != 0 {
		return raw, nil
	}
	return json.Marshal(v)
}

// WriteTo writes the JSON encoding of r to w, as MarshalJSON does, without
// copying the result. It implements [io.WriterTo].
func (r *Response) WriteTo(w io.Writer) (int64, error) {
	if len(r.result) == 0 {
		bits, err := r.MarshalJSON() // no result to copy
		if err != nil {
			return 0, err
		}
		n, err := w.Write(bits)
		return int64(n), err
	}
	head := []byte(`{"jsonrpc":"2.0"`)
	if r.id != "" {
		head = append(append(head, `,"id":`...), r.id...)
	}
	head = append(head, `,"result":`...)
	var n int64
	for _, p := range [][]byte{head, r.result, []byte("}")} {
		m, err := w.Write(p)
		n += int64(m)
		if err != nil {
			return n, err
		}
	}
	return n, nil
}
