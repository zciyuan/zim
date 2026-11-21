package client

import "google.golang.org/grpc"

type fakeTransportClient struct {
	conn *grpc.ClientConn
}
