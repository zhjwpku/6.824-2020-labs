package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"math"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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

	// worker Id
	workerId := os.Getegid()

	// map tasks
	doMapTask(workerId, mapf)

	// reduce jobs
	doReduceTask(workerId, reducef)
}

//
// Do Map tasks
func doMapTask(workerId int, mapf func(string, string) []KeyValue) {
	finishedMapId := ""
	filePaths := []string{}
	for {
		// get filename
		filename, bucketNum := CallGetMapTask(workerId, finishedMapId, filePaths)
		if filename == "no more map task" {
			break
		}
		finishedMapId = ""
		filePaths = filePaths[:0]
		if filename == "some split not finished" {
			time.Sleep(3 * time.Second)
			continue
		}
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v, err %v", filename, err)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v, err %v", filename, err)
		}
		file.Close()

		// do the map function and shuffle
		kva := mapf(filename, string(content))
		buckets := make([][]KeyValue, bucketNum)
		for _, kv := range kva {
			idx := ihash(kv.Key) % bucketNum
			buckets[idx] = append(buckets[idx], kv)
		}

		// write the shuffled intermediate data into files
		files := make([]string, 0)
		for i, bucket := range buckets {
			tmpFilename := fmt.Sprintf("tmpfile-%d-%d", ihash(filename), i)
			files = append(files, tmpFilename)
			ofile, err := os.Create(tmpFilename)
			if err != nil {
				log.Fatalf("cannot create %v, err %v", tmpFilename, err)
			}
			enc := json.NewEncoder(ofile)
			for _, kv := range bucket {
				if err := enc.Encode(&kv); err != nil {
					log.Fatalf("encode kv err %v", err)
				}
			}
			if err := ofile.Close(); err != nil {
				log.Fatalf("close file err %v", err)
			}
		}

		finishedMapId = filename
		filePaths = files
	}
}

//
// Do Reduce tasks
//
func doReduceTask(workerId int, reducef func(string, []string) string) {
	finishedReduceId := -1
	for {
		reduceId, filePaths := CallGetReduceTask(workerId, finishedReduceId)
		if reduceId == -1 {
			break
		}
		finishedReduceId = -1
		if reduceId == math.MaxInt {
			time.Sleep(3 * time.Second)
			continue
		}
		intermediate := []KeyValue{}
		for _, filename := range filePaths {
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				intermediate = append(intermediate, kv)
			}
			if err := file.Close(); err != nil {
				log.Fatalf("close file err %v", err)
			}
		}

		sort.Sort(ByKey(intermediate))

		oname := "mr-out-" + strconv.Itoa(reduceId)
		ofile, _ := os.Create(oname)

		//
		// call Reduce on each distinct key in intermediate[],
		// and print the result to mr-out-0.
		//
		i := 0
		for i < len(intermediate) {
			j := i + 1
			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, intermediate[k].Value)
			}
			output := reducef(intermediate[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

			i = j
		}

		ofile.Close()
		finishedReduceId = reduceId
	}
}

//
// Get Map task from master
//
func CallGetMapTask(workerId int, finishedId string, filePaths []string) (string, int) {
	args := MapTaskArgs{}
	args.WorkerId = workerId
	args.FinishedSplitId = finishedId
	args.FilePaths = filePaths

	reply := MapTaskReply{}

	call("Master.GetMapTask", &args, &reply)

	return reply.SplitId, reply.BucketNum
}

//
// Get Reduce task from master
func CallGetReduceTask(workerId int, finishedId int) (int, []string) {
	args := ReduceTaskArgs{}
	args.WorkerId = workerId
	args.FinishedId = finishedId

	reply := ReduceTaskReply{}

	call("Master.GetReduceTask", &args, &reply)
	return reply.ReduceId, reply.FilePaths
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
