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

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	worker_id := -1
	// ask coordinator for a task
	for {
		reply := MapreduceReply{}
		CallAskForTask(worker_id, &reply)
		worker_id = reply.WorkerId
		log.Printf("worker %v get task %v\n", worker_id, reply.TaskType)
		switch reply.TaskType {
		case "Map":
			map_worker(&reply, mapf, worker_id)
		case "Wait":
			time.Sleep(1 * time.Second)
		case "Reduce":
			reduce_worker(&reply, reducef, worker_id)
		case "Finished":
			return

		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func map_worker(reply *MapreduceReply, mapf func(string, string) []KeyValue, worker_id int) {
	if len(reply.FileName) != 1 {
		log.Fatalf("Map task should have only one input file")
	}
	// load content
	filename := reply.FileName[0]
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v, err %v", filename, err)
	}

	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v, err %v", filename, err)
	}

	file.Close()

	kva := mapf(filename, string(content))
	// save to different intermediate files based on keys
	for _, kv := range kva {
		intermediate_file_name := fmt.Sprintf("mr-%v-%v", worker_id, ihash(kv.Key)%10)
		intermediate_file, err := os.OpenFile(intermediate_file_name, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatalf("cannot open %v,err %v", intermediate_file_name, err)
		}

		enc := json.NewEncoder(intermediate_file)
		err = enc.Encode(&kv)
		if err != nil {
			log.Fatalf("cannot encode %v", kv)
		}

		intermediate_file.Close()

	}

	// imform coordinator that this task is finished
	CallFinishTask(worker_id)
}

func reduce_worker(reply *MapreduceReply, reducef func(string, []string) string, worker_id int) {
	reduce_id := reply.ReduceId
	// find all the related intermediate files whose name ends with reduce_id
	kva := []KeyValue{}
	for i := range reply.MapWorkers {
		filename := fmt.Sprintf("mr-%v-%v", i, reduce_id)
		file, err := os.Open(filename)
		if err == nil {
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kva = append(kva, kv)
			}
			file.Close()
		}
	}

	if len(kva) == 0 {
		// imform coordinator that this task is finished
		CallFinishTask(worker_id)
		return
	}

	// sort the kva
	sort.Sort(ByKey(kva))

	oname := fmt.Sprintf("mr-out-%v", reduce_id)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in kva[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	ofile.Close()

	// imform coordinator that this task is finished
	CallFinishTask(worker_id)

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func CallAskForTask(worker_id int, reply *MapreduceReply) {
	args := MapreduceArgs{worker_id}

	ok := call("Coordinator.AskForTask", &args, &reply)
	if !ok {
		log.Fatalf("disconnct with coordinator\n")
	}
}

func CallFinishTask(worker_id int) {
	args := ExampleArgs{worker_id}
	reply := ExampleReply{}

	ok := call("Coordinator.FinishTask", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
