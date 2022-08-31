package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	log.SetOutput(ioutil.Discard)

	for {
		// map job
		err := map_job(mapf)
		if err != nil {
			log.Fatal(err)
		}

		// reduce job
		done, err := reduce_job(reducef)
		if err != nil {
			log.Fatal(err)
		}
		if done {
			break
		}
	}
}

// map job
func map_job(mapf func(string, string) []KeyValue) error {
	map_reply := CallAskForMap()
	if map_reply.Wait || map_reply.File == "" {
		log.Println("no file to process")
	} else {
		log.Printf("receive workid: %v, filename: %v\n", map_reply.WorkerId, map_reply.File)

		file, err := os.Open(map_reply.File)
		if err != nil {
			return fmt.Errorf("cannot open %v", map_reply.File)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			return fmt.Errorf("cannot read %v", map_reply.File)
		}
		defer file.Close()

		// Process the file.
		kva := mapf(map_reply.File, string(content))

		// Create intermediate files.
		intermediate_filename := "mrinter-" + strconv.Itoa(int(map_reply.WorkerId)) + "-"
		encoders := make([]*json.Encoder, map_reply.Nreduce)
		for i := int64(0); i < map_reply.Nreduce; i++ {
			file, err = os.Create(intermediate_filename + strconv.Itoa(int(i)))
			if err != nil {
				log.Fatalf("cannot open %v", intermediate_filename)
			}
			defer file.Close()
			encoders[i] = json.NewEncoder(file)
		}
		for _, kv := range kva {
			reduce_index := int64(ihash(kv.Key)) % map_reply.Nreduce
			err := encoders[reduce_index].Encode(&kv)
			if err != nil {
				return fmt.Errorf("cannot encode %v in %v", kv, map_reply.File)
			}
		}
		if !CallMapDone(map_reply.TaskId, map_reply.WorkerId) {
			return fmt.Errorf("cannot send map done signal")
		}

		// Done
		log.Printf("Processing %v done\n", map_reply.File)
	}
	return nil
}

// reduce job
func reduce_job(reducef func(string, []string) string) (bool, error) {
	reply := CallAskForReduce()
	if !reply.Ready {
		log.Printf("reduce job is not ready\n")
		return false, nil
	} else if reply.Done {
		log.Printf("reduce job is done\n")
		return true, nil
	} else if reply.Wait {
		log.Printf("reduce job is waiting\n")
		return false, nil
	} else {
		// do reduce job
		log.Printf("get reduce job: %v from intermidiated worker : %v\n", reply.Reduceindex, reply.InterIDs)
	}

	// read all the record
	intermediate_kvs := []KeyValue{}
	for _, workerid := range reply.InterIDs {
		filename := "mrinter-" + strconv.Itoa(int(workerid)) + "-" + strconv.Itoa(int(reply.Reduceindex))
		file, err := os.Open(filename)
		if err != nil {
			return false, fmt.Errorf("cannot open %v", filename)
		}
		decoder := json.NewDecoder(file)
		for {
			var m KeyValue
			if err := decoder.Decode(&m); err == io.EOF {
				break
			} else if err != nil {
				return false, fmt.Errorf("cannot decode %v", filename)
			}
			intermediate_kvs = append(intermediate_kvs, m)
		}
		defer file.Close()
	}
	// sort the record
	sort.Sort(ByKey(intermediate_kvs))

	// build the {key,{value1,value2...}}
	// processing use the reducef function
	// write back the result
	oname := "mr-out-" + strconv.Itoa(int(reply.Reduceindex))
	ofile, err := os.Create(oname)
	if err != nil {
		return false, fmt.Errorf("cannot create %v", oname)
	}

	i := 0
	for i < len(intermediate_kvs) {
		j := i + 1
		for j < len(intermediate_kvs) && intermediate_kvs[j].Key == intermediate_kvs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate_kvs[k].Value)
		}
		output := reducef(intermediate_kvs[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate_kvs[i].Key, output)

		i = j
	}

	defer ofile.Close()

	CallReduceDone(reply.TaskId, reply.WorkerID)
	log.Printf("%v finished \n", oname)
	return false, nil
}

//
// function call ask for file name
func CallAskForMap() AskForMapReply {
	// declare an argument structure.
	args := AskForMapArgs{}
	// declare a reply structure.
	reply := AskForMapReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.AskForFile" tells the
	// receiving server that we'd like to call
	// the AskForFile() method of struct Coordinator.
	ok := call("Coordinator.AskForMap", &args, &reply)
	if !ok {
		log.Printf("CallAskForMap failed!\n")
	}
	return reply
}

//
// function call ask for map done
func CallMapDone(task_id int64, worker_id int64) bool {
	args := MapDoneArgs{worker_id, task_id}
	reply := MapDoneReply{}
	ok := call("Coordinator.MapDone", &args, &reply)
	if !ok {
		log.Printf("CallMapDone failed!\n")
	}
	return ok
}

//
// function call ask for reduce job
func CallAskForReduce() AskForReduceReply {
	args := AskForReduceArgs{}
	reply := AskForReduceReply{}
	ok := call("Coordinator.AskForReduce", &args, &reply)
	if !ok {
		log.Printf("CallAskForReduce failed!\n")
	}
	return reply
}

func CallReduceDone(task_id int64, worker_id int64) bool {
	args := ReduceDoneArgs{worker_id, task_id}
	reply := ReduceDoneReply{}
	ok := call("Coordinator.ReduceDone", &args, &reply)
	if !ok {
		log.Printf("CallReduceDone failed!\n")
	}
	return ok
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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
		log.Printf("reply.Y %v\n", reply.Y)
	} else {
		log.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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

	log.Println(err)
	return false
}
