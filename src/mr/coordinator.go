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
	// Your definitions here.
	nReduce        int
	nMap           int
	files          []string
	mapfinished    int
	maptasklog     []int //0 not allocated 1 wait 2 finished
	reducefinished int
	reducetasklog  []int      //0 not allocated 1 wait 2 finished
	mu             sync.Mutex //lock
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) ReceiveFinishedMap(args *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	c.mapfinished++
	c.maptasklog[args.MapTaskNumber] = 2
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) ReceiveFinishedReduce(args *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	c.reducefinished++
	c.reducetasklog[args.ReduceTaskNumber] = 2
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) AllocateTask(args *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	if c.mapfinished < c.nMap {
		//allocate map task
		allocate := -1
		for i := 0; i < c.nMap; i++ {
			if c.maptasklog[i] == 0 {
				allocate = i
				break
			}
		}
		if allocate == -1 {
			//wait for unfinished map job
			reply.Tasktype = 2
			c.mu.Unlock()
		} else {
			//allocate map task
			reply.NReduce = c.nReduce
			reply.Tasktype = 0
			reply.MapTaskNumber = allocate
			reply.Filename = c.files[allocate]
			c.maptasklog[allocate] = 1
			c.mu.Unlock()
			go func() {
				time.Sleep(time.Duration(10) * time.Second)
				c.mu.Lock()
				if c.maptasklog[allocate] == 1 {
					c.maptasklog[allocate] = 0
				}
				c.mu.Unlock()
			}()
		}
	} else if c.mapfinished == c.nMap && c.reducefinished < c.nReduce {
		//allcoate reduce task
		allocate := -1
		for i := 0; i < c.nReduce; i++ {
			if c.reducetasklog[i] == 0 {
				allocate = i
				break
			}
		}
		if allocate == -1 {
			//wait for unfinished map job
			reply.Tasktype = 2
			c.mu.Unlock()
		} else {
			//allocate map task
			reply.NMap = c.nMap
			reply.Tasktype = 1
			reply.ReduceTaskNumber = allocate
			c.reducetasklog[allocate] = 1
			c.mu.Unlock()
			go func() {
				time.Sleep(time.Duration(10) * time.Second)
				c.mu.Lock()
				if c.reducetasklog[allocate] == 1 {
					c.reducetasklog[allocate] = 0
				}
				c.mu.Unlock()
			}()
		}
	} else {
		reply.Tasktype = 3
		c.mu.Unlock()
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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

	// Your code here.

	ret := c.reducefinished == c.nReduce
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.files = files
	c.nMap = len(files)
	c.nReduce = nReduce
	c.maptasklog = make([]int, c.nMap)
	c.reducetasklog = make([]int, c.nReduce)
	// Your code here.

	c.server()
	return &c
}
