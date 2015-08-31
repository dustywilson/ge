package main

import (
	"net"
	"runtime"
	"time"

	"github.com/coreos/go-etcd/etcd"
	"github.com/dustywilson/ge"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type geServer struct {
	etcdStrong *etcd.Client
	etcdWeak   *etcd.Client
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	l, err := net.Listen("tcp", "127.0.0.1:43434")
	if err != nil {
		panic(err)
	}

	g := &geServer{
		etcdStrong: etcd.NewClient(nil),
		etcdWeak:   etcd.NewClient(nil),
	}
	g.etcdStrong.SetConsistency(etcd.STRONG_CONSISTENCY)
	g.etcdWeak.SetConsistency(etcd.WEAK_CONSISTENCY)

	grpcServer := grpc.NewServer()
	ge.RegisterGeServer(grpcServer, g)
	grpcServer.Serve(l)
}

func (g *geServer) etcd(strongConsistency bool) *etcd.Client {
	if strongConsistency {
		return g.etcdStrong
	}
	return g.etcdWeak
}

func (g *geServer) Create(c context.Context, req *ge.SetRequest) (*ge.Response, error) {
	response, err := g.etcd(req.StrongConsistency).Create(req.Path, req.Value, req.Ttl)
	if err != nil {
		return nil, err
	}
	if response != nil {
		return convertResponse(response), nil
	}
	return nil, nil
}

func (g *geServer) CreateDir(c context.Context, req *ge.SetRequest) (*ge.Response, error) {
	response, err := g.etcd(req.StrongConsistency).CreateDir(req.Path, req.Ttl)
	if err != nil {
		return nil, err
	}
	if response != nil {
		return convertResponse(response), nil
	}
	return nil, nil
}

func (g *geServer) Delete(c context.Context, req *ge.DeleteRequest) (*ge.Response, error) {
	response, err := g.etcd(req.StrongConsistency).Delete(req.Path, req.Recursive)
	if err != nil {
		return nil, err
	}
	if response != nil {
		return convertResponse(response), nil
	}
	return nil, nil
}

func (g *geServer) DeleteDir(c context.Context, req *ge.DeleteRequest) (*ge.Response, error) {
	response, err := g.etcd(req.StrongConsistency).DeleteDir(req.Path)
	if err != nil {
		return nil, err
	}
	if response != nil {
		return convertResponse(response), nil
	}
	return nil, nil
}

func (g *geServer) Get(c context.Context, req *ge.GetRequest) (*ge.Response, error) {
	response, err := g.etcd(req.StrongConsistency).Get(req.Path, req.Sort, req.Recursive)
	if err != nil {
		return nil, err
	}
	if response != nil {
		return convertResponse(response), nil
	}
	return nil, nil
}

func (g *geServer) Set(c context.Context, req *ge.SetRequest) (*ge.Response, error) {
	response, err := g.etcd(req.StrongConsistency).Set(req.Path, req.Value, req.Ttl)
	if err != nil {
		return nil, err
	}
	if response != nil {
		return convertResponse(response), nil
	}
	return nil, nil
}

func (g *geServer) Update(c context.Context, req *ge.SetRequest) (*ge.Response, error) {
	response, err := g.etcd(req.StrongConsistency).Update(req.Path, req.Value, req.Ttl)
	if err != nil {
		return nil, err
	}
	if response != nil {
		return convertResponse(response), nil
	}
	return nil, nil
}

func (g *geServer) UpdateDir(c context.Context, req *ge.SetRequest) (*ge.Response, error) {
	response, err := g.etcd(req.StrongConsistency).UpdateDir(req.Path, req.Ttl)
	if err != nil {
		return nil, err
	}
	if response != nil {
		return convertResponse(response), nil
	}
	return nil, nil
}

func (g *geServer) Watch(req *ge.WatchRequest, stream ge.Ge_WatchServer) error {
	recv := make(chan *etcd.Response)
	stop := make(chan bool)

	go func() {
		for {
			select {
			case <-time.After(time.Second * 10):
				// FIXME: how else can we detect disconnected clients, without writing to them?
				err := stream.Send(&ge.Envelope{Ping: &ge.Ping{Timestamp: time.Now().Unix()}})
				if err != nil {
					close(stop)
					return
				}
			case r, ok := <-recv:
				if !ok {
					close(stop)
					return
				}
				err := stream.Send(&ge.Envelope{Response: convertResponse(r)})
				if err != nil {
					close(stop)
					return
				}
			}
		}
	}()

	_, err := g.etcd(req.StrongConsistency).Watch(req.Prefix, req.WaitIndex, req.Recursive, recv, stop)
	if err != nil {
		return err
	}

	<-stop
	return nil
}

func (g *geServer) WatchOnce(c context.Context, req *ge.WatchRequest) (*ge.Response, error) {
	response, err := g.etcd(req.StrongConsistency).Watch(req.Prefix, req.WaitIndex, req.Recursive, nil, nil)
	if err != nil {
		return nil, err
	}
	if response != nil {
		return convertResponse(response), nil
	}
	return nil, nil
}

func convertResponse(r *etcd.Response) *ge.Response {
	return &ge.Response{
		Action:    r.Action,
		Node:      convertNode(r.Node),
		PrevNode:  convertNode(r.PrevNode),
		EtcdIndex: r.EtcdIndex,
		RaftIndex: r.RaftIndex,
		RaftTerm:  r.RaftTerm,
	}
}

func convertNode(n *etcd.Node) *ge.Node {
	if n == nil {
		return nil
	}
	var expiration int64
	if n.Expiration != nil {
		expiration = n.Expiration.Unix()
	}
	return &ge.Node{
		Path:          n.Key,
		Value:         n.Value,
		Dir:           n.Dir,
		Expiration:    expiration,
		Ttl:           n.TTL,
		Nodes:         convertNodes(n.Nodes),
		ModifiedIndex: n.ModifiedIndex,
		CreatedIndex:  n.CreatedIndex,
	}
}

func convertNodes(ns []*etcd.Node) []*ge.Node {
	nodes := make([]*ge.Node, len(ns))
	for i, n := range ns {
		nodes[i] = convertNode(n)
	}
	return nodes
}
