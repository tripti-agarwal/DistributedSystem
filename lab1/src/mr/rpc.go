package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// type ExampleArgs struct {
// 	X int
// }

// type ExampleReply struct {
// 	Y int
// }

// Add your RPC definitions here.

//argument structure


//The idea od writing the RPC structures is
//For everything there will be args which will act like input from worker
//Reply which will be communicated via RPC from coordinator to worker
//Reply helps in identifying what needs to be done
type Args struct{
//using this for 
	//number of maps args
	//map args
	//reduce args
	//Final done work args

}
type MapTaskFinishedArgs struct{
	MapTask string
	MapFile string
}
type ReduceArgs struct{
	ReduceTask string
}
type ReduceTaskFinishedArgs struct{
	ReduceId int
}




type MapReply struct{
	MapNum int //number of map tasks to do
}
type MapTypeWorkReply struct{
	//file name 
	MapFile string
	MapId int
}
type FinishedReply struct{
	Finished bool
}
type ReduceReply struct{
	ReduceId int //reduce id recieved
}
type ReduceTaskFinishedReply struct{
	 
}



// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
