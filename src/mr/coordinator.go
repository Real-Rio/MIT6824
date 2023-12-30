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
	worker_state        map[int]string // "Map" "Reduce" "Wait" "Finished"
	next_worker_id      int            // every worker has a unique id
	next_map_input      int
	map_workers         map[int]bool // map worker id
	last_task_time      map[int]time.Time
	finished_reduce_num int // number of finished reduce task
	reduce_num          int
	next_reduce_id      int
	mutex               sync.Mutex
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
	if c.Done() {
		reply.TaskType = "Finished"
		return nil
	}

	if args.WorkerId == -1 {
		c.mutex.Lock()
		args.WorkerId = c.next_worker_id
		c.next_worker_id++
		c.mutex.Unlock()
	}

	c.mutex.Lock()
	if c.next_map_input < len(c.input_files) {
		reply.TaskType = "Map"
		reply.WorkerId = args.WorkerId
		reply.FileName = c.input_files[c.next_map_input : c.next_map_input+1]
		c.worker_state[args.WorkerId] = "Map"
		c.last_task_time[reply.WorkerId] = time.Now()
		c.map_workers[args.WorkerId] = true
		c.next_map_input++
		c.mutex.Unlock()
		return nil
	}
	c.mutex.Unlock()

	// check all the map tasks are finished
	var running_worker int
	if !checkMapFinished(c,&running_worker) {
		// check if running worker has run for 10s
		if time.Since(c.last_task_time[running_worker]) > 10*time.Second {
			reply.TaskType = "Map"
			reply.WorkerId = running_worker
		
		
		}
		reply.TaskType = "Wait"
		return nil
	}

	// reduce task
	c.mutex.Lock()
	if c.next_reduce_id < c.reduce_num {
		reply.TaskType = "Reduce"
		reply.ReduceId = c.next_reduce_id
		reply.WorkerId = args.WorkerId
		for key := range c.map_workers {
			reply.MapWorkers = append(reply.MapWorkers, key)
		}
		c.next_reduce_id++
		c.worker_state[args.WorkerId] = "Reduce"
		c.last_task_time[reply.WorkerId] = time.Now()
	}
	c.mutex.Unlock()
	return nil
}

// TODO success: reply.Y = 1 fail: reply.Y = 0
func (c *Coordinator) FinishTask(args *ExampleArgs, reply *ExampleReply) error {
	switch c.worker_state[args.X] {
	case "Map":
		c.mutex.Lock()
		c.worker_state[args.X] = "Wait"
		c.mutex.Unlock()
		reply.Y = 1
	case "Reduce":
		c.mutex.Lock()
		c.worker_state[args.X] = "Finished"
		c.mutex.Unlock()
		c.finished_reduce_num++
		reply.Y = 1
	default:
		log.Fatalf("worker %v finish task error", args.X)
	}
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
	c.mutex.Lock()
	for id, state := range c.worker_state {
		if state == "Map" {
			*running_worker = id
			c.mutex.Unlock()
			return false
		}
	}
	c.mutex.Unlock()
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{input_files: files, worker_state: make(map[int]string), next_worker_id: 0, next_map_input: 0,
		reduce_num: nReduce, next_reduce_id: 0, map_workers: make(map[int]bool), finished_reduce_num: 0}

	// Your code here.

	c.server()
	return &c
}
