package main

import (
	"io"
	"log"
	"os"
	"runtime"
	"time"

	"github.com/dustywilson/ge"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	conn, err := grpc.Dial("127.0.0.1:43434", grpc.WithInsecure())
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	defer conn.Close()

	c := ge.NewGeClient(conn)

	// WATCH
	{
		stream, err := c.Watch(context.Background(), &ge.WatchRequest{
			Prefix:    "/",
			Recursive: true,
		})
		if err != nil {
			log.Printf("Watch Err1: %s", err)
			return
		}
		defer stream.CloseSend()
		go func(stream ge.Ge_WatchClient) {
			for {
				in, err := stream.Recv()
				if err == io.EOF {
					log.Println("Ge_WatchClient EOF")
					return
				}
				if err != nil {
					log.Printf("Watch Err2: %s", err)
					return
				}
				log.Printf("Watch VALUE: %+v", in)
			}
		}(stream)
	}

	// Wait a bit...
	time.Sleep(time.Second * 2)

	// CREATE
	{
		r, err := c.Create(context.Background(), &ge.SetRequest{
			Path:  "/ge-test/something",
			Value: "This is something.",
			Ttl:   5,
		})
		if err != nil {
			log.Printf("Create: %s", err)
			os.Exit(1)
		}
		log.Printf("Create: %+v", r)
	}

	// UPDATE
	{
		r, err := c.Update(context.Background(), &ge.SetRequest{
			Path:  "/ge-test/something",
			Value: "This is something else.",
			Ttl:   15,
		})
		if err != nil {
			log.Printf("Update: %s", err)
			os.Exit(1)
		}
		log.Printf("Update: %+v", r)
	}

	// Wait a bit...
	time.Sleep(time.Second * 30)
}
