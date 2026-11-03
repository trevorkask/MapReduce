package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type taskState int

const (
	idle taskState = iota
	running
	finished
)

type task struct {
	state   taskState
	started time.Time
	attempt int
}

type Coordinator struct {
	mu      sync.Mutex
	files   []string
	nReduce int
	maps    []task
	reduces []task
}

func (c *Coordinator) RequestTask(args *TaskRequest, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	reply.Kind = WaitTask
	reply.NMap = len(c.files)
	reply.NReduce = c.nReduce
	tasks := c.maps
	kind := MapTask
	if allFinished(c.maps) {
		tasks = c.reduces
		kind = ReduceTask
		reply.MapAttempts = make([]int, len(c.maps))
		for id, t := range c.maps {
			reply.MapAttempts[id] = t.attempt
		}
		if allFinished(tasks) {
			reply.Kind = ExitTask
			return nil
		}
	}
	for id := range tasks {
		t := &tasks[id]
		if t.state == idle || (t.state == running && time.Since(t.started) >= 10*time.Second) {
			t.state = running
			t.started = time.Now()
			t.attempt++
			reply.Attempt = t.attempt
			reply.Kind = kind
			reply.ID = id
			if kind == MapTask {
				reply.File = c.files[id]
			}
			return nil
		}
	}
	return nil
}

func (c *Coordinator) CompleteTask(args *TaskComplete, reply *CompleteReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var tasks []task
	switch args.Kind {
	case MapTask:
		tasks = c.maps
	case ReduceTask:
		tasks = c.reduces
	default:
		return nil
	}
	if args.ID < 0 || args.ID >= len(tasks) {
		return nil
	}
	t := &tasks[args.ID]
	if t.state == running && t.attempt == args.Attempt {
		if args.Kind == ReduceTask {
			destination := filepath.Join(filepath.Dir(args.Output), fmt.Sprintf("mr-out-%d", args.ID))
			if err := os.Rename(args.Output, destination); err != nil {
				return err
			}
		}
		t.state = finished
	}
	return nil
}

func allFinished(tasks []task) bool {
	for _, t := range tasks {
		if t.state != finished {
			return false
		}
	}
	return true
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
	c.mu.Lock()
	defer c.mu.Unlock()
	return allFinished(c.maps) && allFinished(c.reduces)
}

func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := &Coordinator{
		files:   append([]string(nil), files...),
		nReduce: nReduce,
		maps:    make([]task, len(files)),
		reduces: make([]task, nReduce),
	}
	c.server(sockname)
	return c
}
