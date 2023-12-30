package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	input_files         []string
	next_worker_id      int // every worker has a unique id
	next_map_input      int
	worker_infos        map[int]workerInfo
	finished_reduce_num int // number of finished reduce task
	reduce_num          int
	next_reduce_id      int
	mutex               sync.Mutex
}

type workerInfo struct {
	last_tast_time      time.Time
	last_map_input_file string
	state               string // "Map" "Reduce" "Wait" "Finished"
	reduce_id           int
	map_id              int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AskForTask(args *MapreduceArgs, reply *MapreduceReply) error {
	c.mutex.Lock()

	if c.Done() {
		reply.TaskType = "Finished"
		c.mutex.Unlock()
		return nil
	}

	if args.WorkerId == -1 {
		args.WorkerId = c.next_worker_id
		c.next_worker_id++
	}

	if c.next_map_input < len(c.input_files) {
		reply.TaskType = "Map"
		reply.WorkerId = args.WorkerId
		reply.MapFile = c.input_files[c.next_map_input]
		reply.ReduceNum = c.reduce_num
		reply.MapId = c.next_map_input
		c.worker_infos[args.WorkerId] = workerInfo{last_tast_time: time.Now(), last_map_input_file: reply.MapFile, state: "Map", map_id: c.next_map_input}
		c.next_map_input++
		c.mutex.Unlock()
		return nil
	}

	// check all the map tasks are finished
	var running_worker int
	if !checkMapFinished(c, &running_worker) {
		// check if running worker has run for 10s
		if time.Since(c.worker_infos[running_worker].last_tast_time) > 10*time.Second {
			reply.TaskType = "Map"
			reply.WorkerId = args.WorkerId
			reply.MapFile = c.worker_infos[running_worker].last_map_input_file
			reply.ReduceNum = c.reduce_num
			reply.MapId = c.worker_infos[running_worker].map_id
			c.worker_infos[args.WorkerId] = workerInfo{last_tast_time: time.Now(), last_map_input_file: reply.MapFile, state: "Map", map_id: reply.MapId}

			delete(c.worker_infos, running_worker)
		} else {
			reply.TaskType = "Wait"
			reply.WorkerId = args.WorkerId
		}
		c.mutex.Unlock()
		return nil
	}

	// reduce task
	if c.next_reduce_id < c.reduce_num {
		reply.TaskType = "Reduce"
		reply.ReduceId = c.next_reduce_id
		reply.WorkerId = args.WorkerId
		reply.MapTaskNum = len(c.input_files)
		c.next_reduce_id++
		c.worker_infos[args.WorkerId] = workerInfo{last_tast_time: time.Now(), state: "Reduce", reduce_id: reply.ReduceId}
		c.mutex.Unlock()
		return nil
	}

	// check if reduce task has run for 10s
	var crash_reduce_worker int
	if checkReduceIfCrash(c, &crash_reduce_worker) {
		reply.TaskType = "Reduce"
		reply.ReduceId = c.worker_infos[crash_reduce_worker].reduce_id
		reply.WorkerId = args.WorkerId
		reply.MapTaskNum = len(c.input_files)
		c.worker_infos[args.WorkerId] = workerInfo{last_tast_time: time.Now(), state: "Reduce", reduce_id: reply.ReduceId}
		delete(c.worker_infos, crash_reduce_worker)
		c.mutex.Unlock()
		return nil
	}
	reply.TaskType = "Wait"
	reply.WorkerId = args.WorkerId
	log.Printf("wait for reduce task finishing\n")
	c.mutex.Unlock()
	return nil
}

func (c *Coordinator) FinishTask(args *ExampleArgs, reply *ExampleReply) error {
	c.mutex.Lock()
	switch c.worker_infos[args.X].state {
	case "Map":
		c.worker_infos[args.X] = workerInfo{state: "Wait"}
	case "Reduce":
		c.worker_infos[args.X] = workerInfo{state: "Finished"}
		c.finished_reduce_num++
	default:
		log.Fatalf("worker %v finish task error", args.X)
	}
	c.mutex.Unlock()
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.finished_reduce_num == c.reduce_num
}

func checkMapFinished(c *Coordinator, running_worker *int) bool {
	for id, info := range c.worker_infos {
		if info.state == "Map" {
			*running_worker = id
			return false
		}
	}
	return true
}

func checkReduceIfCrash(c *Coordinator, running_worker *int) bool {
	for id, info := range c.worker_infos {
		if info.state == "Reduce" && time.Since(info.last_tast_time) > 10*time.Second {
			*running_worker = id
			return true
		}
	}
	return false
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{input_files: files, worker_infos: make(map[int]workerInfo), next_worker_id: 0, next_map_input: 0,
		reduce_num: nReduce, next_reduce_id: 0, finished_reduce_num: 0}

	// Your code here.

	c.server()
	return &c
}
