package mythic_test

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/MythicC2Profiles/discordx/c2runtime/internal/mythic"
	"github.com/MythicC2Profiles/discordx/c2runtime/internal/mythicpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type pushServer struct {
	mythicpb.UnimplementedPushC2Server
	received chan *mythicpb.PushC2MessageFromAgent
}

func (server *pushServer) StartPushC2StreamingOneToMany(stream mythicpb.PushC2_StartPushC2StreamingOneToManyServer) error {
	for {
		message, err := stream.Recv()
		if err != nil {
			return err
		}
		server.received <- message
		if message.GetIngressID() != "" {
			if err := stream.Send(&mythicpb.PushC2MessageFromMythic{
				Success: true, IngressID: message.GetIngressID(), IsIngressReceipt: true,
			}); err != nil {
				return err
			}
		}
	}
}

func TestGRPCConnectorUsesCurrentLaneAndReceiptFields(t *testing.T) {
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	implementation := &pushServer{received: make(chan *mythicpb.PushC2MessageFromAgent, 2)}
	mythicpb.RegisterPushC2Server(server, implementation)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	connector, err := mythic.NewGRPCConnector("bufconn:17444",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := connector.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&mythic.FromAgent{
		C2ProfileName: "discordx", Message: []byte("body"), TrackingID: "dx2:route",
		IngressID: "ingress-1", IngressLane: mythic.Socks,
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case message := <-implementation.received:
		if message.GetIngressID() != "ingress-1" || message.GetIngressLane() != mythicpb.DeliveryLane_SOCKS || string(message.GetMessage()) != "body" {
			t.Fatalf("server received = %#v", message)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not receive ingress")
	}
	receipt, err := stream.Recv()
	if err != nil || !receipt.IsIngressReceipt || receipt.IngressID != "ingress-1" || !receipt.Success {
		t.Fatalf("Recv() = %#v, %v", receipt, err)
	}
	cancel()
	if _, err := stream.Recv(); err == nil || err == io.EOF {
		// A canceled in-memory gRPC stream normally returns a status error, not
		// EOF; either way it must not continue returning messages.
		if err == nil {
			t.Fatal("canceled stream remained readable")
		}
	}
}
