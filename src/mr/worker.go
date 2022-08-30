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
	filename, nreduce := CallAskForFile()
	if filename == "" {
		log.Println("no file to process")
	} else {
		file, err := os.Open(filename)
		if err != nil {
			return fmt.Errorf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			return fmt.Errorf("cannot read %v", filename)
		}
		defer file.Close()

		// Process the file.
		kva := mapf(filename, string(content))

		// Create intermediate files.
		intermediate_filename := filename + "-intermediate-"
		encoders := make([]*json.Encoder, nreduce)
		for i := int64(0); i < nreduce; i++ {
			file, err = os.Create(intermediate_filename + strconv.Itoa(int(i)))
			defer file.Close()
			if err != nil {
				log.Fatalf("cannot open %v", intermediate_filename)
			}
			encoders[i] = json.NewEncoder(file)
		}
		for _, kv := range kva {
			reduce_index := int64(ihash(kv.Key)) % nreduce
			err := encoders[reduce_index].Encode(&kv)
			if err != nil {
				return fmt.Errorf("cannot encode %v in %v", kv, filename)
			}
		}
		if !CallMapDone(filename) {
			return fmt.Errorf("cannot send map done signal")
		}

		// Done
		log.Printf("Processing %v done\n", filename)
	}
	return nil
}

// reduce job
func reduce_job(reducef func(string, []string) string) (bool, error) {
	var reply AskForReduceReply

	reply = CallAskForReduce()
	if !reply.Ready {
		log.Printf("reduce job is not ready\n")
		return false, nil
	} else if reply.Index == -1 {
		log.Printf("reduce job is done\n")
		return true, nil
	} else {
		// do reduce job
		log.Printf("get reduce job: %v intermidiated files: %v\n", reply.Index, reply.Files)
	}

	// read all the record
	intermediate_kvs := []KeyValue{}
	for _, filename := range reply.Files {
		filename = filename + "-intermediate-" + strconv.Itoa(int(reply.Index))
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
	oname := "mr-out-" + strconv.Itoa(int(reply.Index))
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
	log.Printf("%v finished \n", oname)
	return false, nil
}

//
// function call ask for file name
func CallAskForFile() (string, int64) {
	// declare an argument structure.
	args := AskForFileArgs{}
	// declare a reply structure.
	reply := AskForFileReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.AskForFile" tells the
	// receiving server that we'd like to call
	// the AskForFile() method of struct Coordinator.
	ok := call("Coordinator.AskForFile", &args, &reply)
	if ok {
		// reply.file_name should be 100.
		log.Printf("receive reply.file_name %v\n", reply.File)
	} else {
		log.Printf("CallAskForFile failed!\n")
	}
	return reply.File, reply.Nreduce
}

//
// function call ask for map done
func CallMapDone(name string) bool {
	args := MapDoneArgs{name}
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
