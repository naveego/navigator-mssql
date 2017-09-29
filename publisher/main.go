package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/naveego/api/types/pipeline"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/naveego/api/pipeline/publisher"
	"github.com/naveego/navigator-go/publishers/protocol"
	"github.com/naveego/navigator-go/publishers/server"
	"github.com/naveego/pipeline-publishers/sql/mssql"
	"github.com/sirupsen/logrus"
)

var (
	verbose = flag.Bool("v", false, "enable verbose logging")
)

func main() {

	logrus.SetOutput(os.Stdout)

	if len(os.Args) < 2 {
		fmt.Println("Not enough arguments.")
		os.Exit(-1)
	}

	flag.Parse()

	addr := os.Args[1]

	if *verbose {
		logrus.SetLevel(logrus.DebugLevel)
	}

	srv := server.NewPublisherServer(addr, &publisherHandler{})

	err := srv.ListenAndServe()
	if err != nil {
		logrus.Fatal("Error shutting down server: ", err)
	}
}

type publisherHandler struct {
	publisher publisher.Publisher
	shapes    pipeline.ShapeDefinitions
}

func (h *publisherHandler) TestConnection(request protocol.TestConnectionRequest) (protocol.TestConnectionResponse, error) {
	pub := h.getPublisher()
	ctx := publisher.Context{}

	success, msg, err := pub.TestConnection(ctx)
	if err != nil {
		return protocol.TestConnectionResponse{}, err
	}

	return protocol.TestConnectionResponse{
		Success: success,
		Message: msg,
	}, nil
}

func (h *publisherHandler) DiscoverShapes(request protocol.DiscoverShapesRequest) (protocol.DiscoverShapesResponse, error) {
	logrus.Debugf("Calling Discover Shapes: %v", request.Settings)
	pub := h.getPublisher()
	ctx := publisher.Context{
		Settings: request.Settings,
	}

	shapes, err := pub.Shapes(ctx)
	if err != nil {
		return protocol.DiscoverShapesResponse{}, err
	}

	return protocol.DiscoverShapesResponse{
		Shapes: shapes,
	}, nil
}

func (h *publisherHandler) Init(request protocol.InitRequest) (protocol.InitResponse, error) {
	pub := h.getPublisher()
	ctx := publisher.Context{
		Settings: request.Settings,
	}

	err := pub.Init(ctx)
	if err != nil {
		return protocol.InitResponse{}, err
	}

	shapes, err := pub.Shapes(ctx)
	if err != nil {
		return protocol.InitResponse{}, err
	}

	h.shapes = shapes

	return protocol.InitResponse{}, nil
}

func (h *publisherHandler) Dispose(request protocol.DisposeRequest) (protocol.DisposeResponse, error) {
	pub := h.getPublisher()
	ctx := publisher.Context{}

	err := pub.Dispose(ctx)

	if err != nil {
		return protocol.DisposeResponse{}, err
	}

	return protocol.DisposeResponse{}, nil
}

func (h *publisherHandler) Publish(request protocol.PublishRequest, client protocol.PublisherClient) (protocol.PublishResponse, error) {
	logrus.Debug("Calling publish")
	pub := h.getPublisher()
	ctx := publisher.Context{
		Logger: logrus.WithField("dev", "dev"),
	}

	var shapeDef pipeline.ShapeDefinition
	logrus.Debugf("Looking for Shape: %s in %d shapes", request.ShapeName, len(h.shapes))
	for _, s := range h.shapes {
		if s.Name == request.ShapeName {
			shapeDef = s
		}
	}

	logrus.Debugf("Publshing Shape: %s", shapeDef.Name)
	pub.Publish(ctx, shapeDef, &clientTransport{client})

	return protocol.PublishResponse{Success: true, Message: "Published data points"}, nil
}

func (h *publisherHandler) getPublisher() publisher.Publisher {
	if h.publisher == nil {
		h.publisher = mssql.NewPublisher()
	}

	return h.publisher
}

type clientTransport struct {
	client protocol.PublisherClient
}

func (ct *clientTransport) Send(dataPoints []pipeline.DataPoint) error {
	req := protocol.SendDataPointsRequest{
		DataPoints: dataPoints,
	}

	_, err := ct.client.SendDataPoints(req)
	return err
}

func (ct *clientTransport) Done() error {
	_, err := ct.client.Done(protocol.DoneRequest{})
	return err
}
