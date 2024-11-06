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

type Bykey []KeyValue

func (a Bykey) Len() int           { return len(a) }
func (a Bykey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a Bykey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		args := WorkerArgs{}
		reply := WorkerReply{}
		ok := call("Coordinator.AllocateTask", &args, &reply)
		if !ok || reply.Tasktype == 3 {
			break
		}
		switch reply.Tasktype {
		case 0:
			// map task
			intermediate := []KeyValue{}
			// open && read the file
			content, err := os.ReadFile(reply.Filename)
			if err != nil {
				log.Fatalf("cannot read %v: %v", reply.Filename, err)
			}
			// call mapf
			kva := mapf(reply.Filename, string(content))
			intermediate = append(intermediate, kva...)
			// hash into buckets
			buckets := make([][]KeyValue, reply.NReduce)
			for _, kva := range intermediate {
				buckets[ihash(kva.Key)%reply.NReduce] = append(buckets[ihash(kva.Key)%reply.NReduce], kva)
			}
			// write into intermediate files
			for i, bucket := range buckets {
				oname := fmt.Sprintf("mr-%d-%d", reply.MapTaskNumber, i)
				ofile, err := os.CreateTemp("", oname+"*")
				if err != nil {
					log.Fatalf("cannot create temp file for %v: %v", oname, err)
				}
				enc := json.NewEncoder(ofile)
				for _, kva := range bucket {
					err := enc.Encode(&kva)
					if err != nil {
						log.Fatalf("cannot write into %v: %v", oname, err)
					}
				}
				err = os.Rename(ofile.Name(), oname)
				if err != nil {
					log.Fatalf("cannot rename %v to %v: %v", ofile.Name(), oname, err)
				}
				ofile.Close()
			}
			// call master to send the finish message
			finishedArgs := WorkerArgs{MapTaskNumber: reply.MapTaskNumber, ReduceTaskNumber: -1}
			finishedReply := WorkerReply{}
			call("Coordinator.ReceiveFinishedMap", &finishedArgs, &finishedReply)
		case 1:
			//reduce task
			//collect reduce from mr-x-y
			var intermediate []KeyValue
			for i := 0; i < reply.NMap; i++ {
				iname := fmt.Sprintf("mr-%d-%d", i, reply.ReduceTaskNumber)
				//open &read file
				file, err := os.Open(iname)
				if err != nil {
					log.Fatalf("cannot open %v", iname)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						if err == io.EOF {
							break
						}
						log.Fatalf("cannot decode %v", iname)
					}
					intermediate = append(intermediate, kv)
				}
				file.Close()
			}
			//sort by key
			sort.Slice(intermediate, func(i, j int) bool {
				return intermediate[i].Key < intermediate[j].Key
			})
			//output file
			oname := fmt.Sprintf("mr-out-%d", reply.ReduceTaskNumber)
			ofile, err := os.CreateTemp("", oname+"*")
			if err != nil {
				log.Fatalf("cannot output %v", oname)
			}
			// call Reduce on each distinct key in intermediate[],
			// and print the result to mr-out-0.
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				var values []string
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)
				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
				i = j
			}
			err = os.Rename(ofile.Name(), oname)
			if err != nil {
				log.Fatalf("cannot rename %v to %v: %v", ofile.Name(), oname, err)
			}
			ofile.Close()
			for i := 0; i < reply.NMap; i++ {
				iname := fmt.Sprintf("mr-%d-%d", i, reply.ReduceTaskNumber)
				err := os.Remove(iname)
				if err != nil {
					log.Fatalf("cannot remove %v: %v", iname, err)
				}
			}
			// send the finish message to master
			finishedArgs := WorkerArgs{MapTaskNumber: -1, ReduceTaskNumber: reply.ReduceTaskNumber}
			finishedReply := WorkerReply{}
			call("Coordinator.ReceiveFinishedReduce", &finishedArgs, &finishedReply)
		}
		time.Sleep(time.Second)
	}
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
