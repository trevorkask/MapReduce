package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type taskState int

const (
	idle taskState = iota
	running
	finished
)

type Coordinator struct {
	mu      sync.Mutex
	files   []string
	nReduce int
	maps    []taskState
}

func (c *Coordinator) RequestTask(args *TaskRequest, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	reply.Kind = WaitTask
	reply.NMap = len(c.files)
	reply.NReduce = c.nReduce
	for id, state := range c.maps {
		if state == idle {
			c.maps[id] = running
			reply.Kind = MapTask
			reply.ID = id
			reply.File = c.files[id]
			return nil
		}
	}
	return nil
}

func (c *Coordinator) CompleteTask(args *TaskComplete, reply *CompleteReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.Kind == MapTask && args.ID >= 0 && args.ID < len(c.maps) {
		c.maps[args.ID] = finished
	}
	return nil
}

func (c *Coordinator) server(sockname string) {
	server := rpc.NewServer()
	if err := server.Register(c); err != nil {
		log.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.Handle(rpc.DefaultRPCPath, server)
	os.Remove(sockname)
	listener, err := net.Listen("unix", sockname)
	if err != nil {
		log.Fatal(err)
	}
	go http.Serve(listener, mux)
}

func (c *Coordinator) Done() bool {
	return false
}

func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := &Coordinator{
		files:   append([]string(nil), files...),
		nReduce: nReduce,
		maps:    make([]taskState, len(files)),
	}
	c.server(sockname)
	return c
}
