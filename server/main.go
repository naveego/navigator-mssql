package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/Sirupsen/logrus"
	_ "github.com/denisenkom/go-mssqldb"
	"github.com/naveego/api/pipeline/publisher"
	"github.com/naveego/navigator-go/publishers/protocol"
	"github.com/naveego/navigator-go/publishers/server"
	"github.com/naveego/pipeline-publishers/sql/mssql"
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

type publisherHandler struct{}

func (h *publisherHandler) TestConnection(request protocol.TestConnectionRequest) (protocol.TestConnectionResponse, error) {
	pub := mssql.NewPublisher()
	ctx := publisher.Context{}

	success, msg, err := pub.TestConnection(ctx, request.Settings)
	if err != nil {
		return protocol.TestConnectionResponse{}, err
	}

	return protocol.TestConnectionResponse{
		Success: success,
		Message: msg,
	}, nil
}

func (h *publisherHandler) DiscoverShapes(request protocol.DiscoverShapesRequest) (protocol.DiscoverShapesResponse, error) {
	pub := mssql.NewPublisher()
	ctx := publisher.Context{
		PublisherInstance: request.PublisherInstance,
	}

	shapes, err := pub.Shapes(ctx)
	if err != nil {
		return protocol.DiscoverShapesResponse{}, err
	}

	return protocol.DiscoverShapesResponse{
		Shapes: shapes,
	}, nil
}
