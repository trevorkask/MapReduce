package mr

import (
	"hash/fnv"
	"net/rpc"
	"time"
)

type KeyValue struct {
	Key   string
	Value string
}

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		reply := TaskReply{}
		if !call(sockname, "Coordinator.RequestTask", &TaskRequest{}, &reply) {
			return
		}
		switch reply.Kind {
		case ExitTask:
			return
		default:
			time.Sleep(500 * time.Millisecond)
		}
	}
}

func call(sockname string, rpcname string, args interface{}, reply interface{}) bool {
	client, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		return false
	}
	defer client.Close()
	return client.Call(rpcname, args, reply) == nil
}
