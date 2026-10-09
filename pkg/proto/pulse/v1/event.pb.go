package pulsev1

import (
	"context"
	"time"

	"github.com/pulse/pulse-engine/internal/domain"
	"google.golang.org/grpc"
)

type EventProto struct {
	EventId         string            `protobuf:"bytes,1,opt,name=event_id,json=eventId,proto3" json:"event_id,omitempty"`
	MetricKey       string            `protobuf:"bytes,2,opt,name=metric_key,json=metricKey,proto3" json:"metric_key,omitempty"`
	TimestampUnixMs int64             `protobuf:"varint,3,opt,name=timestamp_unix_ms,json=timestampUnixMs,proto3" json:"timestamp_unix_ms,omitempty"`
	Value           float64           `protobuf:"fixed64,4,opt,name=value,proto3" json:"value,omitempty"`
	Dimensions      map[string]string `protobuf:"bytes,5,rep,name=dimensions,proto3" json:"dimensions,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
}

func (e *EventProto) ToDomain() domain.Event {
	return domain.Event{
		EventID:    e.EventId,
		MetricKey:  e.MetricKey,
		Timestamp:  time.UnixMilli(e.TimestampUnixMs).UTC(),
		Value:      e.Value,
		Dimensions: e.Dimensions,
	}
}

type IngestRequest struct {
	Events []*EventProto `protobuf:"bytes,1,rep,name=events,proto3" json:"events,omitempty"`
}

type IngestResponse struct {
	AcceptedCount uint32   `protobuf:"varint,1,opt,name=accepted_count,json=acceptedCount,proto3" json:"accepted_count,omitempty"`
	RejectedCount uint32   `protobuf:"varint,2,opt,name=rejected_count,json=rejectedCount,proto3" json:"rejected_count,omitempty"`
	Errors        []string `protobuf:"bytes,3,rep,name=errors,proto3" json:"errors,omitempty"`
}

type IngestServiceClient interface {
	IngestEvents(ctx context.Context, in *IngestRequest, opts ...grpc.CallOption) (*IngestResponse, error)
}

type IngestServiceServer interface {
	IngestEvents(context.Context, *IngestRequest) (*IngestResponse, error)
}

func RegisterIngestServiceServer(s grpc.ServiceRegistrar, srv IngestServiceServer) {
	s.RegisterService(&_IngestService_serviceDesc, srv)
}

var _IngestService_serviceDesc = grpc.ServiceDesc{
	ServiceName: "pulse.v1.IngestService",
	HandlerType: (*IngestServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "IngestEvents",
			Handler: func(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
				in := new(IngestRequest)
				if err := dec(in); err != nil {
					return nil, err
				}
				if interceptor == nil {
					return srv.(IngestServiceServer).IngestEvents(ctx, in)
				}
				info := &grpc.UnaryServerInfo{
					Server:     srv,
					FullMethod: "/pulse.v1.IngestService/IngestEvents",
				}
				handler := func(ctx context.Context, req interface{}) (interface{}, error) {
					return srv.(IngestServiceServer).IngestEvents(ctx, req.(*IngestRequest))
				}
				return interceptor(ctx, in, info, handler)
			},
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "pkg/proto/pulse/v1/event.proto",
}
