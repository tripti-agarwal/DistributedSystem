package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"

import "encoding/json"
import "sort"
import "time"
//

type KeyValue struct {
	Key   string
	Value string
}
// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
// Map functions return a slice of KeyValue.



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
	mapargs := Args{}
	mapreply := MapReply{}
	call("Coordinator.NumberOfMapTasks", &mapargs, &mapreply)
	
	numberOfMapTasks := mapreply.MapNum  
	//fmt.Printf("%d", numberOfMapTasks)
	for (true){
		
			// declare an argument structure
			mapWorkargs := Args{} //using same maprArgs struct as no input args to pass
			mapWorkReply := MapTypeWorkReply{}
			call("Coordinator.MapWork", &mapWorkargs, &mapWorkReply)
			mapFilename := mapWorkReply.MapFile
			mapId := mapWorkReply.MapId
			// fmt.Printf("%s", mapFilename)
			// fmt.Printf("%d", mapId)

			if mapFilename == ""{
				if getMapTaskFinished(mapFilename) {
					// 	fmt.Printf("I am inside")
					break
				}
				time.Sleep(time.Second)
				// mapFinishArgs2 := MapTaskFinishedArgs{}
				// mapFinishArgs2.MapFile = "''
				// mapFinishReply2 := MapTaskFinishedReply{}
				// call("Coordinator.MappingFinished", &mapFinishArgs2, &mapFinishReply2)
				// if !mapFinishReply2.MaybeFinished{
				
				// 	break
				// }
				// time.Sleep(time.Second)
			} else if mapFilename != ""{
				//copied from mrsequential 
				file, err := os.Open(mapFilename)
				if err != nil {
					log.Fatalf("cannot open %v", mapFilename)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", mapFilename)
				}
				kva := mapf(mapFilename, string(content)) //changed based on our current input
				//now we perform partition and save in different files
				//save the result produce by map in the intermediate files
				//The nReduce in mrcoordinator is 10
				intermediatejsonfiles := make([]*os.File, 10)
				for i := 0; i < 10; i++ {
					newfilename := fmt.Sprintf("mr-%d-%d", mapId, i)
					//newfilename := "mr-"+strconv.Itoa(numberOfMapTasks)+"-"+strconv.Itoa(i)
					//fmt.Printf("after new filename is obtained")
					intermediatejsonfiles[i], _ = os.Create(newfilename)
					defer intermediatejsonfiles[i].Close()
				}
				
				for _, keyvalue := range kva {
					//perform partitioning
					partitionNum := ihash(keyvalue.Key) % 10   //this partition number is also reduce job number
					enc := json.NewEncoder(intermediatejsonfiles[partitionNum])
					//some how this part leads to error
					// jsonfileErr := enc.Encode(&keyvalue)
					// if jsonfileErr != nil {
					// 	log.Fatal("error: ",err)
					// }
					enc.Encode(&keyvalue)
					
				}

				//map work done
				//pass the intermediate files to the coordinator
				//so that its assigned to reduce

				getMapTaskFinished(mapFilename)
				// mapFinishArgs1 := MapTaskFinishedArgs{}
				// mapFinishArgs1.MapFile = mapFilename
				// mapFinishReply1 := MapTaskFinishedReply{}
				// call("Coordinator.MappingFinished", &mapFinishArgs1, &mapFinishReply1)

			} 
	}
	for (true){
		reduceArgs := ReduceArgs{}
		reduceReply := ReduceReply{}
		call("Coordinator.ReduceTask", &reduceArgs, &reduceReply)
		reduceTaskId := reduceReply.ReduceId

		if reduceTaskId != -1 {
			kva := make([]KeyValue, 0)
			for i := 0; i < numberOfMapTasks; i++ {
				tempFileName := fmt.Sprintf("mr-%d-%d",i, reduceTaskId)

				file, err := os.Open(tempFileName)
				//defer file.Close()
				if err != nil {
					log.Fatalf("cannot open %v", tempFileName)
				}

				//decoder function to read the json file
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
			}
			//combining the reduce results now
			//copied from mrsequential
			reduceFileName := fmt.Sprintf("mr-out-%d",reduceTaskId)
			ofile, _ := os.Create(reduceFileName)
			defer ofile.Close()
			//ordering guarantees
			sort.Sort(ByKey(kva))

			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

				i = j
			}
			
			reduceFinishArgs := ReduceTaskFinishedArgs{}
			reduceFinishArgs.ReduceId = reduceTaskId
			reduceFinishReply := ReduceTaskFinishedReply{}
			call("Coordinator.ReduceFinished", &reduceFinishArgs, &reduceFinishReply)
		} else if reduceTaskId == -1{
			doneArgs := Args{}
			doneReply := FinishedReply{}
			call("Coordinator.FullyFinished", &doneArgs, &doneReply)
			done := doneReply.Finished
			if (done){
				break
			}
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	//CallExample()
}
		
			
		
func getMapTaskFinished(fileName string) bool {

	args := MapTaskFinishedArgs{}
	args.MapFile = fileName
	reply := FinishedReply{}
	call("Coordinator.MapTaskFinished", &args, &reply)
	return reply.Finished
}

	


//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func CallExample() {

// 	// declare an argument structure.
// 	args := ExampleArgs{}

// 	// fill in the argument(s).
// 	args.X = 99

// 	// declare a reply structure.
// 	reply := ExampleReply{}

// 	// send the RPC request, wait for the reply.
// 	// the "Coordinator.Example" tells the
// 	// receiving server that we'd like to call
// 	// the Example() method of struct Coordinator.
// 	ok := call("Coordinator.Example", &args, &reply)
// 	if ok {
// 		// reply.Y should be 100.
// 		fmt.Printf("reply.Y %v\n", reply.Y)
// 	} else {
// 		fmt.Printf("call failed!\n")
// 	}
// }

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
	return err == nil
	// if err == nil {
	// 	return true
	// }

	// fmt.Println(err)
	// return false
}

