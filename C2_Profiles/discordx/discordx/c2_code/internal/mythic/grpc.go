package mythic

import (
	"context"
	"errors"
	"net"
	"strings"

	"github.com/MythicC2Profiles/discordx/c2runtime/internal/mythicpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type GRPCConnector struct {
	address string
	options []grpc.DialOption
}

func NewGRPCConnector(address string, options ...grpc.DialOption) (*GRPCConnector, error) {
	address = strings.TrimSpace(address)
	if address == "" || strings.Contains(address, "://") {
		return nil, errors.New("Mythic Push C2 address is invalid")
	}
	if host, port, err := net.SplitHostPort(address); err != nil || host == "" || port == "" {
		return nil, errors.New("Mythic Push C2 address is invalid")
	}
	if len(options) == 0 {
		options = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}
	return &GRPCConnector{address: address, options: append([]grpc.DialOption(nil), options...)}, nil
}

func (connector *GRPCConnector) Open(ctx context.Context) (Stream, error) {
	connection, err := grpc.NewClient("passthrough:///"+connector.address, connector.options...)
	if err != nil {
		return nil, errors.New("Mythic Push C2 connection failed")
	}
	stream, err := mythicpb.NewPushC2Client(connection).StartPushC2StreamingOneToMany(ctx)
	if err != nil {
		_ = connection.Close()
		return nil, errors.New("Mythic Push C2 stream failed")
	}
	go func() {
		<-ctx.Done()
		_ = connection.Close()
	}()
	return &grpcStream{connection: connection, stream: stream}, nil
}

type grpcStream struct {
	connection *grpc.ClientConn
	stream     mythicpb.PushC2_StartPushC2StreamingOneToManyClient
}

func (stream *grpcStream) Send(message *FromAgent) error {
	return stream.stream.Send(&mythicpb.PushC2MessageFromAgent{
		C2ProfileName: message.C2ProfileName,
		Message:       append([]byte(nil), message.Message...), Base64Message: append([]byte(nil), message.Base64Message...),
		TrackingID: message.TrackingID, IngressID: message.IngressID,
		IngressLane: mythicpb.DeliveryLane(message.IngressLane), OutboundID: message.OutboundID,
		IsOutboundReceipt: message.IsOutboundReceipt, OutboundSuccess: message.OutboundSuccess,
	})
}

func (stream *grpcStream) Recv() (*FromMythic, error) {
	message, err := stream.stream.Recv()
	if err != nil {
		_ = stream.connection.Close()
		return nil, err
	}
	return &FromMythic{
		Success: message.GetSuccess(), Error: message.GetError(), Message: append([]byte(nil), message.GetMessage()...),
		TrackingID: message.GetTrackingID(), DeliveryLane: Lane(message.GetDeliveryLane()),
		IngressID: message.GetIngressID(), IsIngressReceipt: message.GetIsIngressReceipt(),
		OutboundID: message.GetOutboundID(),
	}, nil
}
