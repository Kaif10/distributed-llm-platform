// Package server exposes a store.Store over gRPC.
//
// This layer is deliberately thin. All the interesting guarantees live in
// the store; the server only translates requests and maps errors to gRPC
// status codes. Keeping it thin is what lets Phase 2 swap the single-node
// store for a Raft-replicated one without touching clients.
package server

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	kvv1 "dsys/gen/kv/v1"
	"dsys/kv/store"
)

// Server implements kvv1.KVServer.
type Server struct {
	kvv1.UnimplementedKVServer
	st *store.Store
}

// New wraps a store.
func New(st *store.Store) *Server { return &Server{st: st} }

func (s *Server) Put(_ context.Context, req *kvv1.PutRequest) (*kvv1.PutResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "empty key")
	}
	if err := s.st.Put(req.Key, req.Value, req.Meta); err != nil {
		return nil, toStatus(err)
	}
	return &kvv1.PutResponse{}, nil
}

func (s *Server) Get(_ context.Context, req *kvv1.GetRequest) (*kvv1.GetResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "empty key")
	}
	v, ok := s.st.Get(req.Key)
	return &kvv1.GetResponse{Value: v, Found: ok}, nil
}

func (s *Server) Delete(_ context.Context, req *kvv1.DeleteRequest) (*kvv1.DeleteResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "empty key")
	}
	existed, err := s.st.Delete(req.Key, req.Meta)
	if err != nil {
		return nil, toStatus(err)
	}
	return &kvv1.DeleteResponse{Existed: existed}, nil
}

func (s *Server) CompareAndSwap(_ context.Context, req *kvv1.CASRequest) (*kvv1.CASResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "empty key")
	}
	swapped, cur, err := s.st.CompareAndSwap(req.Key, req.Expected, req.ExpectAbsent, req.Value, req.Meta)
	if err != nil {
		return nil, toStatus(err)
	}
	return &kvv1.CASResponse{Swapped: swapped, Current: cur}, nil
}

// toStatus maps store errors to gRPC codes. Clients use the code to decide
// whether a retry is safe: Unavailable/Internal are retryable (with the
// same RequestMeta!), InvalidArgument/Unimplemented are not.
func toStatus(err error) error {
	switch {
	case errors.Is(err, store.ErrNotImplemented):
		return status.Error(codes.Unimplemented, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
