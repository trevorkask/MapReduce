package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
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
		case MapTask:
			if err := runMap(reply, mapf); err != nil {
				log.Print(err)
				return
			}
			done := TaskComplete{Kind: MapTask, ID: reply.ID}
			if !call(sockname, "Coordinator.CompleteTask", &done, &CompleteReply{}) {
				return
			}
		case ReduceTask:
			if err := runReduce(reply, reducef); err != nil {
				log.Print(err)
				return
			}
			done := TaskComplete{Kind: ReduceTask, ID: reply.ID}
			if !call(sockname, "Coordinator.CompleteTask", &done, &CompleteReply{}) {
				return
			}
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

func runMap(task TaskReply, mapf func(string, string) []KeyValue) error {
	contents, err := os.ReadFile(task.File)
	if err != nil {
		return err
	}
	buckets := make([][]KeyValue, task.NReduce)
	for _, kv := range mapf(task.File, string(contents)) {
		bucket := ihash(kv.Key) % task.NReduce
		buckets[bucket] = append(buckets[bucket], kv)
	}
	for bucket, pairs := range buckets {
		name := fmt.Sprintf("mr-%d-%d", task.ID, bucket)
		if err := writeMapFile(name, pairs); err != nil {
			return err
		}
	}
	return nil
}

func writeMapFile(name string, pairs []KeyValue) error {
	file, err := os.CreateTemp(".", "mr-map-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	enc := json.NewEncoder(file)
	for _, kv := range pairs {
		if err := enc.Encode(&kv); err != nil {
			file.Close()
			return err
		}
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(file.Name(), name)
}

func runReduce(task TaskReply, reducef func(string, []string) string) error {
	var pairs []KeyValue
	for id := 0; id < task.NMap; id++ {
		file, err := os.Open(fmt.Sprintf("mr-%d-%d", id, task.ID))
		if err != nil {
			return err
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err == io.EOF {
				break
			}
			if err != nil {
				file.Close()
				return err
			}
			pairs = append(pairs, kv)
		}
		if err := file.Close(); err != nil {
			return err
		}
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].Key < pairs[j].Key })
	file, err := os.CreateTemp(".", "mr-reduce-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	for i := 0; i < len(pairs); {
		j := i + 1
		values := []string{pairs[i].Value}
		for j < len(pairs) && pairs[j].Key == pairs[i].Key {
			values = append(values, pairs[j].Value)
			j++
		}
		if _, err := fmt.Fprintf(file, "%v %v\n", pairs[i].Key, reducef(pairs[i].Key, values)); err != nil {
			file.Close()
			return err
		}
		i = j
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(file.Name(), fmt.Sprintf("mr-out-%d", task.ID))
}
