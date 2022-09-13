package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"




type Coordinator struct {
	// Your definitions here.
	MapNum int
	MapTasks chan string  //channel for map task file name channel
	
	MapId int
	MapTaskId chan int //map task number channel
	MapTaskStatus map[string]bool
	MapMutex sync.Mutex 


	ReduceTaskId chan int //reduce task number channel
	ReduceMutex sync.Mutex 
	ReduceTaskStatus map[int]bool


	DoneMutex sync.Mutex
	DoneWork bool
}

func (c *Coordinator) NumberOfMapTasks(args *Args, reply *MapReply) error {
	//reply.Y = args.X + 1
	//this will be the first call asking for number of map tasks to perform from worker
	//once we have this we can move further and ask
	//worker to either perform reduce or map
	reply.MapNum = c.MapNum
	return nil
}

func (c *Coordinator) MapWork(args *Args, reply *MapTypeWorkReply) error {
	//reply.Y = args.X + 1
	select {
	case reply.MapFile = <- c.MapTasks:
		reply.MapId = <- c.MapTaskId
		filename := reply.MapFile 
		mapTaskId := reply.MapId 
		//communicate what to do with the current map id with the coordinator map id and map files 
		go c.mapworkerfunction(filename, mapTaskId)
	default:
		reply.MapFile = ""
		reply.MapId = 0
	}
	return nil
}

func  (c *Coordinator) mapworkerfunction(mapfilename string, mapTaskId int) {
	//wait for 10 secs
	
		time.Sleep(10 * time.Second)
		//write atomic instruction so lock for each
		c.MapMutex.Lock()
		if(c.MapTaskStatus[mapfilename] == false){
			//we need to assign file and id to the channel
			c.MapTasks <- mapfilename
			c.MapTaskId <- mapTaskId
		}
		//else we do not have to worry about anything //hopefully
		//now unlock so that no context switch happens while assigning work
		c.MapMutex.Unlock()
	
}
func (c *Coordinator) MapTaskFinished(args *MapTaskFinishedArgs, reply *FinishedReply) error {
	if args.MapFile != "" {
		c.MapMutex.Lock()
		mapfilename := args.MapFile
		//just set that file to true
		c.MapTaskStatus[mapfilename] = true
		c.MapMutex.Unlock()
	}else if args.MapFile == "" {
		
		reply.Finished = true
		c.MapMutex.Lock()
		for _, v := range c.MapTaskStatus {
			if !v {
				reply.Finished = false
				break
			}
		}
		c.MapMutex.Unlock()
	}

	return nil
}

func (c *Coordinator) ReduceTask(args *ReduceArgs, reply *ReduceReply) error {
	//reply.Y = args.X + 1
	
	select{
	case reply.ReduceId = <- c.ReduceTaskId:
		reduceId := reply.ReduceId
		//communicate what to do with the current reduce id with the coordinator reduce id
		go c.reduceworkerfunction(reduceId)
	default:
		reply.ReduceId = -1
	}
	return nil
}

func (c *Coordinator) reduceworkerfunction(reduceTaskId int) {
	//wait for 10 secs
	
		time.Sleep(10 * time.Second)
		//write atomic instruction so lock for each
		c.ReduceMutex.Lock()
		if(c.ReduceTaskStatus[reduceTaskId] == false){
			//we need to assign file and id to the channel
			//c.mapTasks <- mapfilename
			c.ReduceTaskId <- reduceTaskId
		}
		//else we do not have to worry about anything //hopefully
		//now unlock so that no context switch happens while assigning work
		c.ReduceMutex.Unlock()
	
}
func (c *Coordinator) ReduceFinished(args *ReduceTaskFinishedArgs, reply *ReduceTaskFinishedReply) error {
	//reply.Y = args.X + 1
	c.ReduceMutex.Lock()
	reduceTaskId := args.ReduceId 
	c.ReduceTaskStatus[reduceTaskId] = true
	c.ReduceMutex.Unlock()
	return nil
}


func (c *Coordinator) FullyFinished(args *Args, reply *FinishedReply) error {
	//reply.Y = args.X + 1
	c.DoneMutex.Lock()
	finish := c.DoneWork
	reply.Finished = finish
	c.DoneMutex.Unlock()	
	return nil
}
// Your code here -- RPC handlers for the worker to call.
//
//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
// 	reply.Y = args.X + 1
// 	return nil
// }


//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := true
	//if any of the reduce task is still set to false we will return the value as false
	//else it is true
	// Your code here.
	c.ReduceMutex.Lock()
	//reduceStatus := c.reduceTaskStatus
	for _, v := range c.ReduceTaskStatus {
		if !v{
			ret = false
		}
	}
	c.ReduceMutex.Unlock()

	//now we can set the ret value for the finally finished 
	//ret value based on newly updated value

	c.DoneMutex.Lock()
	c.DoneWork = ret
	c.DoneMutex.Unlock()

	

	
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.MapNum = len(files) //number of pg files

	//map work
	c.MapTasks = make(chan string, len(files)) //map file channel
	c.MapTaskId = make(chan int, len(files))  //map id channel
	
	c.MapTaskStatus = make(map[string]bool)  //to set the map task status

	//now set the initial values for all the above defined variables to the initial value
	for i, v := range files {  
		c.MapTasks <- v
		c.MapTaskId <- i
		c.MapTaskStatus[v] = false
	}

	//reduce work
	c.ReduceTaskId = make(chan int, nReduce) //reduce id channel
	c.ReduceTaskStatus = make(map[int]bool)  //to set the reduce task status
	for i := 0; i < nReduce; i++ {
		c.ReduceTaskId <- i
		c.ReduceTaskStatus[i] = false
	}


	c.server()
	return &c
}

