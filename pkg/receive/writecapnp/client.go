// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package writecapnp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"

	"capnproto.org/go/capnp/v3"
	"capnproto.org/go/capnp/v3/exc"
	"capnproto.org/go/capnp/v3/rpc"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	pkgerrors "github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/thanos-io/thanos/pkg/store/storepb"
)

type Dialer interface {
	Dial() (net.Conn, error)
}

type TCPDialer struct {
	address string
}

func NewTCPDialer(address string) *TCPDialer {
	return &TCPDialer{address: address}
}

func (t TCPDialer) Dial() (net.Conn, error) {
	addr, err := net.ResolveTCPAddr("tcp", t.address)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		return nil, pkgerrors.Wrapf(err, "failed to dial peer %s", t.address)
	}
	return conn, nil
}

func (t TCPDialer) String() string {
	return t.address
}

type RemoteWriteClient struct {
	mu sync.Mutex

	dialer Dialer
	conn   *rpc.Conn

	writer Writer
	logger log.Logger
	target string
}

func NewRemoteWriteClient(dialer Dialer, logger log.Logger) *RemoteWriteClient {
	return &RemoteWriteClient{
		dialer: dialer,
		logger: logger,
		target: describeDialer(dialer),
	}
}

func (r *RemoteWriteClient) RemoteWrite(ctx context.Context, in *storepb.WriteRequest, _ ...grpc.CallOption) (*storepb.WriteResponse, error) {
	return r.writeWithReconnect(ctx, 2, in)
}

func (r *RemoteWriteClient) writeWithReconnect(ctx context.Context, numReconnects int, in *storepb.WriteRequest) (*storepb.WriteResponse, error) {
	if err := r.connect(ctx); err != nil {
		return nil, err
	}

	result, release := r.writer.Write(ctx, func(params Writer_write_Params) error {
		wr, err := params.NewWr()
		if err != nil {
			return err
		}
		return BuildInto(wr, in.Tenant, in.Timeseries)
	})
	defer release()

	s, err := result.Struct()
	if err != nil {
		if numReconnects > 0 && shouldReconnect(err) {
			level.Warn(r.logger).Log("msg", "rpc failed, reconnecting", "endpoint", r.target)
			if err := r.Close(); err != nil {
				return nil, err
			}
			numReconnects--
			return r.writeWithReconnect(ctx, numReconnects, in)
		}
		return nil, pkgerrors.Wrap(err, "failed writing to peer")
	}
	switch s.Error() {
	case WriteError_unavailable:
		return nil, status.Error(codes.Unavailable, "rpc failed")
	case WriteError_alreadyExists:
		return nil, status.Error(codes.AlreadyExists, "rpc failed")
	case WriteError_invalidArgument:
		return nil, status.Error(codes.InvalidArgument, "rpc failed")
	case WriteError_internal:
		return nil, status.Error(codes.Internal, "rpc failed")
	default:
		return &storepb.WriteResponse{}, nil
	}
}

func (r *RemoteWriteClient) connect(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.conn != nil {
		return nil
	}

	conn, err := r.dialer.Dial()
	if err != nil {
		return pkgerrors.Wrap(err, "failed to dial peer")
	}
	r.conn = rpc.NewConn(rpc.NewPackedStreamTransport(conn), nil)
	writer := Writer(r.conn.Bootstrap(ctx))
	if err := writer.Resolve(ctx); err != nil {
		level.Warn(r.logger).Log("msg", "failed to bootstrap capnp writer, closing connection", "endpoint", r.target, "err", err)
		r.closeUnlocked()
		return pkgerrors.Wrap(err, "failed to bootstrap capnp writer")
	}

	r.writer = writer
	return nil
}

func describeDialer(d Dialer) string {
	if stringer, ok := d.(fmt.Stringer); ok {
		return stringer.String()
	}
	return ""
}

func (r *RemoteWriteClient) Close() error {
	r.mu.Lock()
	r.closeUnlocked()
	r.mu.Unlock()
	return nil
}

func (r *RemoteWriteClient) closeUnlocked() {
	if r.conn != nil {
		conn := r.conn
		r.conn = nil
		go conn.Close()
	}
}

// ShouldReconnect reports whether a remote write error should trigger a reconnect.
func ShouldReconnect(err error) bool {
	if err == nil {
		return false
	}
	if capnp.IsDisconnected(err) {
		return true
	}
	if errors.Is(err, rpc.ErrConnClosed) {
		return true
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var excErr *exc.Exception
	if errors.As(err, &excErr) {
		if errors.Is(excErr, context.DeadlineExceeded) {
			return true
		}
		if errors.Is(excErr.Unwrap(), context.DeadlineExceeded) {
			return true
		}
		if strings.Contains(excErr.Error(), context.DeadlineExceeded.Error()) {
			return true
		}
	}
	return false
}

func shouldReconnect(err error) bool {
	return ShouldReconnect(err)
}
