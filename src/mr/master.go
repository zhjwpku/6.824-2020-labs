package mr

import (
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Master struct {
	// Your definitions here.
	mu sync.Mutex

	// map tasks
	nMap            int
	mapTasks        map[string]*MapTask
	finishedMapTask int

	// reduce tasks
	nReduce            int
	reduceTasks        map[int]*ReduceTask
	finishedReduceTask int
}

type MapTask struct {
	status    TaskStatus
	lastTime  time.Time
	workerIds map[int]time.Time
}

type ReduceTask struct {
	status    TaskStatus
	files     []string
	lastTime  time.Time
	workerIds map[int]time.Time
}

type TaskStatus int64

const (
	Init TaskStatus = iota
	Started
	Finished
)

// Your code here -- RPC handlers for the worker to call.
func (m *Master) GetMapTask(args *MapTaskArgs, reply *MapTaskReply) error {
	if args.FinishedSplitId != "" {
		// update intermediate file path
		updateIntermediateFilePaths(m, args)
	}
	reply.BucketNum = m.nReduce
	reply.SplitId = "some split not finished"
	if m.finishedMapTask == m.nMap {
		reply.SplitId = "no more map task"
	} else {
		m.mu.Lock()
		for k, v := range m.mapTasks {
			if v.status == Init {
				reply.SplitId = k
				v.status = Started
				v.workerIds[args.WorkerId] = time.Now()
				v.lastTime = v.workerIds[args.WorkerId]
				break
			}
		}
		// reassign timeout tasks
		for k, v := range m.mapTasks {
			if v.status == Started && time.Since(v.lastTime) > 10*time.Second {
				reply.SplitId = k
				v.workerIds[args.WorkerId] = time.Now()
				v.lastTime = v.workerIds[args.WorkerId]
			}
		}
		m.mu.Unlock()
	}
	return nil
}

func (m *Master) GetReduceTask(args *ReduceTaskArgs, reply *ReduceTaskReply) error {
	if args.FinishedId >= 0 {
		updateFinishedReduceTask(m, args.FinishedId)
	}
	reply.ReduceId = math.MaxInt
	if m.finishedReduceTask == m.nReduce {
		// this imply the worker should exit
		reply.ReduceId = -1
	} else {
		m.mu.Lock()
		for k, v := range m.reduceTasks {
			if v.status == Init {
				reply.ReduceId = k
				reply.FilePaths = v.files
				v.status = Started
				v.workerIds[args.WorkerId] = time.Now()
				v.lastTime = v.workerIds[args.WorkerId]
				break
			}
		}
		// reassign timeout tasks
		for k, v := range m.reduceTasks {
			if v.status == Started && time.Since(v.lastTime) > 10*time.Second {
				reply.ReduceId = k
				reply.FilePaths = v.files
				v.workerIds[args.WorkerId] = time.Now()
				v.lastTime = v.workerIds[args.WorkerId]
			}
		}
		m.mu.Unlock()
	}
	return nil
}

func updateIntermediateFilePaths(m *Master, args *MapTaskArgs) {
	m.mu.Lock()
	if m.mapTasks[args.FinishedSplitId].status == Started {
		m.mapTasks[args.FinishedSplitId].status = Finished
		m.finishedMapTask++
		for k, v := range args.FilePaths {
			// fmt.Printf("k: %v v: %v\n", k, v)
			m.reduceTasks[k].files = append(m.reduceTasks[k].files, v)
		}
	}
	m.mu.Unlock()
}

func updateFinishedReduceTask(m *Master, finishedId int) {
	m.mu.Lock()
	if m.reduceTasks[finishedId].status == Started {
		m.reduceTasks[finishedId].status = Finished
		m.finishedReduceTask++
	}
	m.mu.Unlock()
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	if m.finishedReduceTask == m.nReduce {
		ret = true
	}

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	m.nMap = len(files)
	m.mapTasks = make(map[string]*MapTask)
	m.finishedMapTask = 0
	m.nReduce = nReduce
	m.reduceTasks = make(map[int]*ReduceTask)
	m.finishedReduceTask = 0

	for _, file := range files {
		m.mapTasks[file] = &MapTask{Init, time.Unix(1<<63-62135596801, 999999999), make(map[int]time.Time)}
	}

	for i := 0; i < nReduce; i++ {
		m.reduceTasks[i] = &ReduceTask{Init, make([]string, 0), time.Unix(1<<63-62135596801, 999999999), make(map[int]time.Time)}
	}

	m.server()
	return &m
}
