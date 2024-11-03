// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.5.1
// - protoc             v5.28.2
// source: proto/rpc.proto

package proto

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.64.0 or later.
const _ = grpc.SupportPackageIsVersion9

const (
	ReplicaServer_Read_FullMethodName             = "/ReplicaServer/Read"
	ReplicaServer_Write_FullMethodName            = "/ReplicaServer/Write"
	ReplicaServer_Prepare_FullMethodName          = "/ReplicaServer/Prepare"
	ReplicaServer_Commit_FullMethodName           = "/ReplicaServer/Commit"
	ReplicaServer_ReceiveHeartBeat_FullMethodName = "/ReplicaServer/ReceiveHeartBeat"
	ReplicaServer_StartViewChange_FullMethodName  = "/ReplicaServer/StartViewChange"
)

// ReplicaServerClient is the client API for ReplicaServer service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type ReplicaServerClient interface {
	Read(ctx context.Context, in *ReadRequest, opts ...grpc.CallOption) (*ReadReply, error)
	Write(ctx context.Context, in *WriteRequest, opts ...grpc.CallOption) (*WriteReply, error)
	Prepare(ctx context.Context, in *PrepareRequest, opts ...grpc.CallOption) (*PrepareOKReply, error)
	Commit(ctx context.Context, in *CommitRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
	ReceiveHeartBeat(ctx context.Context, in *HeartBeatRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
	StartViewChange(ctx context.Context, in *StartViewChangeRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
}

type replicaServerClient struct {
	cc grpc.ClientConnInterface
}

func NewReplicaServerClient(cc grpc.ClientConnInterface) ReplicaServerClient {
	return &replicaServerClient{cc}
}

func (c *replicaServerClient) Read(ctx context.Context, in *ReadRequest, opts ...grpc.CallOption) (*ReadReply, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(ReadReply)
	err := c.cc.Invoke(ctx, ReplicaServer_Read_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *replicaServerClient) Write(ctx context.Context, in *WriteRequest, opts ...grpc.CallOption) (*WriteReply, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(WriteReply)
	err := c.cc.Invoke(ctx, ReplicaServer_Write_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *replicaServerClient) Prepare(ctx context.Context, in *PrepareRequest, opts ...grpc.CallOption) (*PrepareOKReply, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(PrepareOKReply)
	err := c.cc.Invoke(ctx, ReplicaServer_Prepare_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *replicaServerClient) Commit(ctx context.Context, in *CommitRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, ReplicaServer_Commit_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *replicaServerClient) ReceiveHeartBeat(ctx context.Context, in *HeartBeatRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, ReplicaServer_ReceiveHeartBeat_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *replicaServerClient) StartViewChange(ctx context.Context, in *StartViewChangeRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, ReplicaServer_StartViewChange_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ReplicaServerServer is the server API for ReplicaServer service.
// All implementations must embed UnimplementedReplicaServerServer
// for forward compatibility.
type ReplicaServerServer interface {
	Read(context.Context, *ReadRequest) (*ReadReply, error)
	Write(context.Context, *WriteRequest) (*WriteReply, error)
	Prepare(context.Context, *PrepareRequest) (*PrepareOKReply, error)
	Commit(context.Context, *CommitRequest) (*emptypb.Empty, error)
	ReceiveHeartBeat(context.Context, *HeartBeatRequest) (*emptypb.Empty, error)
	StartViewChange(context.Context, *StartViewChangeRequest) (*emptypb.Empty, error)
	mustEmbedUnimplementedReplicaServerServer()
}

// UnimplementedReplicaServerServer must be embedded to have
// forward compatible implementations.
//
// NOTE: this should be embedded by value instead of pointer to avoid a nil
// pointer dereference when methods are called.
type UnimplementedReplicaServerServer struct{}

func (UnimplementedReplicaServerServer) Read(context.Context, *ReadRequest) (*ReadReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Read not implemented")
}
func (UnimplementedReplicaServerServer) Write(context.Context, *WriteRequest) (*WriteReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Write not implemented")
}
func (UnimplementedReplicaServerServer) Prepare(context.Context, *PrepareRequest) (*PrepareOKReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Prepare not implemented")
}
func (UnimplementedReplicaServerServer) Commit(context.Context, *CommitRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Commit not implemented")
}
func (UnimplementedReplicaServerServer) ReceiveHeartBeat(context.Context, *HeartBeatRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReceiveHeartBeat not implemented")
}
func (UnimplementedReplicaServerServer) StartViewChange(context.Context, *StartViewChangeRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method StartViewChange not implemented")
}
func (UnimplementedReplicaServerServer) mustEmbedUnimplementedReplicaServerServer() {}
func (UnimplementedReplicaServerServer) testEmbeddedByValue()                       {}

// UnsafeReplicaServerServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to ReplicaServerServer will
// result in compilation errors.
type UnsafeReplicaServerServer interface {
	mustEmbedUnimplementedReplicaServerServer()
}

func RegisterReplicaServerServer(s grpc.ServiceRegistrar, srv ReplicaServerServer) {
	// If the following call pancis, it indicates UnimplementedReplicaServerServer was
	// embedded by pointer and is nil.  This will cause panics if an
	// unimplemented method is ever invoked, so we test this at initialization
	// time to prevent it from happening at runtime later due to I/O.
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&ReplicaServer_ServiceDesc, srv)
}

func _ReplicaServer_Read_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ReadRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ReplicaServerServer).Read(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ReplicaServer_Read_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ReplicaServerServer).Read(ctx, req.(*ReadRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ReplicaServer_Write_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(WriteRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ReplicaServerServer).Write(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ReplicaServer_Write_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ReplicaServerServer).Write(ctx, req.(*WriteRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ReplicaServer_Prepare_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PrepareRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ReplicaServerServer).Prepare(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ReplicaServer_Prepare_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ReplicaServerServer).Prepare(ctx, req.(*PrepareRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ReplicaServer_Commit_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CommitRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ReplicaServerServer).Commit(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ReplicaServer_Commit_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ReplicaServerServer).Commit(ctx, req.(*CommitRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ReplicaServer_ReceiveHeartBeat_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(HeartBeatRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ReplicaServerServer).ReceiveHeartBeat(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ReplicaServer_ReceiveHeartBeat_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ReplicaServerServer).ReceiveHeartBeat(ctx, req.(*HeartBeatRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ReplicaServer_StartViewChange_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(StartViewChangeRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ReplicaServerServer).StartViewChange(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ReplicaServer_StartViewChange_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ReplicaServerServer).StartViewChange(ctx, req.(*StartViewChangeRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// ReplicaServer_ServiceDesc is the grpc.ServiceDesc for ReplicaServer service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var ReplicaServer_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "ReplicaServer",
	HandlerType: (*ReplicaServerServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Read",
			Handler:    _ReplicaServer_Read_Handler,
		},
		{
			MethodName: "Write",
			Handler:    _ReplicaServer_Write_Handler,
		},
		{
			MethodName: "Prepare",
			Handler:    _ReplicaServer_Prepare_Handler,
		},
		{
			MethodName: "Commit",
			Handler:    _ReplicaServer_Commit_Handler,
		},
		{
			MethodName: "ReceiveHeartBeat",
			Handler:    _ReplicaServer_ReceiveHeartBeat_Handler,
		},
		{
			MethodName: "StartViewChange",
			Handler:    _ReplicaServer_StartViewChange_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "proto/rpc.proto",
}
