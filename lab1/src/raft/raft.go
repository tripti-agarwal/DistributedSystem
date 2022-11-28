package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
    "6.824/labrpc"
    "sync/atomic"
    "math/rand"
    "sync"
    "time"
    //"fmt"
)

import "bytes"
import "6.824/labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
    CommandValid bool
    Command      interface{}
    CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
    Command interface{}
    Term    int
}

const (
    State_Follower           = 1
    State_Candidate    = 2
    State_Leader       = 3
    
)


//
// A Go object implementing a single Raft peer.
//
type Raft struct {
    mu        sync.Mutex          // Lock to protect shared access to this peer's state
    peers     []*labrpc.ClientEnd // RPC end points of all peers
    persister *Persister          // Object to hold this peer's persisted state
    me        int                 // this peer's index into peers[]
    dead      int32               // set by Kill()
    // Your data here (2A, 2B, 2C).
	//Persistent state on all servers
	//Added in 2A
    CurrentTerm int        //latest term server has seen (initialized to 0 on first boot, increases monotonically)
    VotedFor    int        //candidateId that received vote in current term (or null if none)
    Log         []LogEntry //log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	//Volatile state on all servers:
	//Added in 2B
    CommitIndex int //index of highest log entry known to be committed (initialized to 0, increases monotonically)
    LastApplied int //index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	//Volatile state on leaders:(Reinitialized after election)
	//Added in 2B
    NextIndex  []int //for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
    MatchIndex []int //for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)


    // state a Raft server must maintain.
    //Added in 2A
    State          int           
    VoteCount      int           
    
	//to maintain time on each server
	//Added in 2A
	ElectionTime  *time.Timer   
    HeartbeatTime *time.Timer 

	//Added in 2B
	ApplyCh        chan ApplyMsg 
    
    
    
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
    var term int
    var isleader bool
    // Your code here (2A).
    rf.mu.Lock()
    defer rf.mu.Unlock()
    term = rf.CurrentTerm
	isleader = false
	if rf.State == State_Leader{
		isleader = true
	}
    return term, isleader
}

func (rf *Raft) persist() {
    // Your code here (2C).
    //Added for 2C
    // Example:
    w := new(bytes.Buffer)
    e := labgob.NewEncoder(w)
    e.Encode(rf.CurrentTerm)
    e.Encode(rf.VotedFor)
    e.Encode(rf.Log)
    data := w.Bytes()
    rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    if data == nil || len(data) < 1 { // bootstrap without any state?
        return
    }
    //Added for 2C
    // Your code here (2C).
    // Example:
    r := bytes.NewBuffer(data)
    d := labgob.NewDecoder(r)
    var CurrentTerm int
    var VotedFor int
    var Log []LogEntry
    if d.Decode(&CurrentTerm) != nil ||
       d.Decode(&VotedFor) != nil ||
       d.Decode(&Log) != nil {
    //   error...
    } else {
      rf.CurrentTerm = CurrentTerm
      rf.VotedFor = VotedFor
      rf.Log = Log
    }
}
//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
    // Your data here (2A, 2B).
	//Added in 2A
    Term         int //candidate’s term
    CandidateId  int //candidate requesting vote

	//Added in 2B
    LastLogIndex int //index of candidate’s last log entry (§5.4)
    LastLogTerm  int //term of candidate’s last log entry (§5.4)
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
    // Your data here (2A).
	//Added in 2A
    Term        int  //currentTerm, for candidate to update itself
    VoteGranted bool //true means candidate received vote
}



func (rf *Raft) sendAppendEntriesBroadcast(peerid int){
    rf.mu.Lock()
    lastLogIndex := len(rf.Log) - 1

    //args for RequestVote
    args := RequestVoteArgs{}
    args.Term = rf.CurrentTerm
    args.CandidateId =  rf.me
    args.LastLogIndex = lastLogIndex
    args.LastLogTerm = rf.Log[lastLogIndex].Term
    
    rf.mu.Unlock()


    reply := RequestVoteReply{}
    ok := rf.sendRequestVote(peerid, &args, &reply)

    //reply not ok return , else perform the rest of the stuff
    if !ok{
        return
    }else{

        rf.mu.Lock()
        defer rf.mu.Unlock()


        if reply.Term > rf.CurrentTerm {
            //fmt.Println("current term=", reply.Term)
            rf.CurrentTerm = reply.Term
            if rf.State != State_Follower{

                //step up or down based on the case
                rf.State = State_Follower

                rf.VotedFor = -1
                
                //Stop turns off a ticker. After Stop, no more ticks will be sent. 
                //Stop does not close the channel, to prevent a concurrent goroutine reading from the channel from seeing an erroneous "tick".
                
                rf.HeartbeatTime.Stop()
                
                //Reset stops a ticker and resets its period to the specified duration. 
                //The next tick will arrive after the new period elapses. The duration d must be greater than zero; if not, Reset will panic.
                
                rf.ElectionTime.Reset(time.Duration(130 + rand.Intn(130)*3) * time.Millisecond)
                
                
            }
            rf.persist()
        }
        if reply.VoteGranted == true && rf.State == State_Candidate {
            rf.VoteCount++
            //fmt.Println("Vote count=",rf.VoteCount)
            if rf.VoteCount > len(rf.peers)/2 {
                if rf.State != State_Leader{
                    //fmt.Println("State=", rf.State)
                    rf.State = State_Leader

                    for index := 0; index < len(rf.NextIndex); index++ {
                        //fmt.Println("index=",index)
                        //fmt.Println(len(rf.Log))
                        rf.NextIndex[index] = len(rf.Log)
                    }

                    for index := 0; index < len(rf.MatchIndex); index++ {
                        //fmt.Println("index=",index)
                        rf.MatchIndex[index] = 0
                    }
                    //
                    //Stop the election ticker

                    rf.ElectionTime.Stop()

                    for peerid := 0; peerid < len(rf.peers); peerid++ {
                        
                        if peerid != rf.me {
                           //fmt.Println("send intial append entries for =", peerid")

                            go rf.sendinitialAppendEntries(peerid)  //go routine to send initial entries that we need to append
                       
                        }
                    }

                    //Reset heartbeat
                    rf.HeartbeatTime.Reset(130 * time.Millisecond)
                }
            }
        }
    }
}


func (rf *Raft) sendinitialAppendEntries(peerid int) {
    rf.mu.Lock()
    
    if rf.State == State_Leader{

        prevLogIndex := rf.NextIndex[peerid] - 1

        //fmt.Println("previous log index=",prevLogIndex)

        logindex := prevLogIndex + 1
        loglength := len(rf.Log[(logindex):]) 

        //fmt.Println("log index =", logindex)
        //fmt.Println("log length=", loglength)

        entries := make([]LogEntry, loglength)
        copy(entries, rf.Log[(logindex):])
        //fmt.Println("log copied")


        //args for Append Entries
        args := AppendEntriesArgs{}

        args.Term = rf.CurrentTerm
        args.LeaderId = rf.me

        args.PrevLogIndex = prevLogIndex
        args.PrevLogTerm = rf.Log[(prevLogIndex)].Term

        args.Entries = entries
        args.LeaderCommit = rf.CommitIndex
        
        rf.mu.Unlock()

        reply := AppendEntriesReply{}

        ok := rf.sendAppendEntries(peerid, &args, &reply) 
        if !ok{
            return
        }else{
            rf.mu.Lock()

            if rf.State != State_Leader {
                rf.mu.Unlock()
                return
            }

            // If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
            if reply.Success == false {

                // If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)

                if  rf.CurrentTerm < reply.Term {
                    rf.CurrentTerm = reply.Term
                    if rf.State != State_Follower{
                        //step up or down based on the case
                        rf.State = State_Follower

                        rf.VotedFor = -1
                        
                        rf.HeartbeatTime.Stop()
                        rf.ElectionTime.Reset(time.Duration(130 + rand.Intn(130)*3) * time.Millisecond)
                        
                    }
                    rf.persist()
                } else {
                    
                    //rf.NextIndex[peerid] = args.PrevLogIndex - 1
                    rf.NextIndex[peerid] = reply.NotCommittedIndex

                    if reply.NotCommittedTerm == -1{

                    }else{
                        for index := args.PrevLogIndex; index >= 1; index--{
                            mylogterm := rf.Log[index - 1].Term

                            if reply.NotCommittedTerm != mylogterm{

                            }else{
                                rf.NextIndex[peerid] = index 
                                break
                            }
                        }
                    }
                }
            }else {

                // If successful: update nextIndex and matchIndex for follower (§5.3)

                entrylength := len(args.Entries)

                //fmt.Println("entry length=", entrylength)
                rf.MatchIndex[peerid] = args.PrevLogIndex + entrylength

                rf.NextIndex[peerid] = rf.MatchIndex[peerid] + 1

                //fmt.Println("match index of peerid =", peerid, rf.MatchIndex[peerid])
                //fmt.Println("next index of peerid =", peerid, rf.NextIndex[peerid])

                // If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
                //here N is the commit index

                loglength := len(rf.Log)
                //fmt.Println("current log length=", loglength)
                for commitindex := loglength - 1; commitindex > rf.CommitIndex; commitindex-- {

                    matchcount := 0
                    //fmt.Println("Finding the number of match index which are less than commit index")
                    for matchindex := 0; matchindex < len(rf.MatchIndex); matchindex++ {

                        if commitindex > rf.MatchIndex[matchindex]{
                            //do nothing
                        }else if rf.MatchIndex[matchindex] >= commitindex{

                            //increase the count for the index being less than commit index
                            matchcount++
                        }
                    }
                    //fmt.Println("matchcount =", matchcount)

                    currentpeerlength := len(rf.peers)
                    //fmt.Println("Is math count > current peer length ")
                    if matchcount > currentpeerlength/2 {
                        //fmt.Println("Yes")
                        //when the nodes have same log then we can safely commit the values
                        
                        rf.CommitIndex = commitindex
                        // apply all entries between lastApplied and committed
                        if rf.CommitIndex > rf.LastApplied {

                            lastapplied := rf.LastApplied + 1
                            lastcommitindex := rf.CommitIndex + 1
                            //fmt.Println("Log applied on last applied index =", lastapplied)
                            //fmt.Println("Log last commited on on last commit index =", lastcommitindex)

                            entriesToApply := append([]LogEntry{}, rf.Log[(lastapplied):(lastcommitindex)]...)
                            //fmt.Println("entries applied at index", rf.CommitIndex)

                            go rf.applyEntryRPC(rf.LastApplied+1, entriesToApply)
                        }
                        break
                    }
                }

            } 
            
            rf.mu.Unlock()
        }
    }else{
            rf.mu.Unlock()
            return
    }
}



func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
    // Your code here (2A, 2B).
    rf.mu.Lock()
    defer rf.mu.Unlock()

    
    //Added in 2A

	// Reply false if term < currentTerm

    if args.Term < rf.CurrentTerm {
        reply.Term = rf.CurrentTerm
        reply.VoteGranted = false
        return
    }



	// Reply false if votedFor is not null or not candidateId
    // and candidate’s log is not at 
    //least as up-to-date as receiver’s log
	//This is just a safe case, to remove the extra possibility of getting votes


	if args.Term == rf.CurrentTerm && rf.VotedFor != -1 && rf.VotedFor != args.CandidateId{
        
		reply.Term = rf.CurrentTerm
        reply.VoteGranted = false
        return
	}

	// Step down if term > currentTerm 
    //and set the currentterm to args term
	if args.Term > rf.CurrentTerm {
        rf.CurrentTerm = args.Term


		//In this case we basically step down to follower, 
        //if the state is already not a follower


        if rf.State != State_Follower{

            //step up or down based on the case
            rf.State = State_Follower
            rf.VotedFor = -1
            //stop heart beat ticker
            rf.HeartbeatTime.Stop()

            //reset election time
			rf.ElectionTime.Reset(time.Duration(130 + rand.Intn(130)*3) * time.Millisecond)
			
        }
        rf.persist()

    }

    //If votedFor is null or candidateId, 
    //and candidate’s log is at least as 
    //up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	//Added in 2B


    lastestLogIndex := len(rf.Log) - 1

    if args.LastLogTerm == rf.Log[lastestLogIndex].Term && args.LastLogIndex < (lastestLogIndex){

		reply.Term = rf.CurrentTerm
        reply.VoteGranted = false
        return
	}


    if args.LastLogTerm < rf.Log[lastestLogIndex].Term{
        
        reply.Term = rf.CurrentTerm
        reply.VoteGranted = false
        return
    }


	

    rf.VotedFor = args.CandidateId
    rf.persist()
    reply.Term = rf.CurrentTerm
    reply.VoteGranted = true

    
    rf.ElectionTime.Reset(time.Duration(130 + rand.Intn(130)*3) * time.Millisecond)
}

type AppendEntriesArgs struct {
	//Added in 2A
    Term         int        //leader’s term
    LeaderId     int        //so follower can redirect clients

    PrevLogIndex int        //index of log entry immediately preceding new ones
    PrevLogTerm  int        //term of prevLogIndex entry

    Entries      []LogEntry //log entries to store

    LeaderCommit int        //leader’s commitIndex
}

type AppendEntriesReply struct {
	//Added in 2A
    Term    int  //currentTerm, for leader to update itself
    Success bool //true if follower contained entry matching prevLogIndex and prevLogTerm

    NotCommittedTerm  int //these are basically used to find which term/index are creating trouble in the log
    NotCommittedIndex int
}

func (rf *Raft) applyEntryRPC(startIndex int, entries []LogEntry) {
    for index := 0; index < len(entries); index++  {
         
        myMsg := ApplyMsg{}

        myMsg.CommandValid = true
        myMsg.Command = entries[index].Command
        myMsg.CommandIndex = startIndex + index

        rf.ApplyCh <- myMsg

        rf.mu.Lock()

        if rf.LastApplied < myMsg.CommandIndex {

            rf.LastApplied = myMsg.CommandIndex
        }
        rf.mu.Unlock()
    }
}

//create append entriesRPC
//Append Entries RPC
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    
    reply.Success = true

    // Reply false if term < currentTerm (§5.1)
	//Added in 2A
    if args.Term < rf.CurrentTerm {
        reply.Success = false
        reply.Term = rf.CurrentTerm
        return
    }
    // Step down if term > currentTerm and set the currentterm to args term (§5.1)
	//Added in 2A
    if args.Term > rf.CurrentTerm {
        rf.CurrentTerm = args.Term
        if rf.State != State_Follower {

            //step up or down based on the case
            rf.State = State_Follower
            rf.VotedFor = -1

            //Stop heartbeat ticker
            rf.HeartbeatTime.Stop()

            //Reset election time
			rf.ElectionTime.Reset(time.Duration(130 + rand.Intn(130)*3) * time.Millisecond)
			
        }
        rf.persist()
    }

    // reset election time
    rf.ElectionTime.Reset(time.Duration(130 + rand.Intn(130)*3) * time.Millisecond)

    // Reply false if log doesn’t
    // contain an entry at prevLogIndex
    // whose term matches prevLogTerm
	//Added in 2B
    
    if len(rf.Log) - 1 < args.PrevLogIndex {
        //fmt.Print("log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm");
        //fmtPrintln(args.PrevLogIndex)
        reply.Success = false
        reply.Term = rf.CurrentTerm

        reply.NotCommittedIndex = len(rf.Log)
        reply.NotCommittedTerm = -1 
        return
    }

    // 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
	//Added in 2B
    //index := args.PrevLogIndex
    if rf.Log[(args.PrevLogIndex)].Term != args.PrevLogTerm {
        //fmt.Println("existing entry conflicts with a new one")
        
        reply.Success = false
        reply.Term = rf.CurrentTerm

        checkterm := rf.Log[args.PrevLogIndex].Term
        reply.NotCommittedTerm = checkterm

        checkindex := args.PrevLogIndex
        for reply.NotCommittedTerm == rf.Log[checkindex-1].Term{
            checkindex--
        }

        reply.NotCommittedIndex = checkindex

        
        return
    }

    // 4. Append any new entries not already in the log
	//In this we need to first look for entries that do not match the entries in the follower log
	//Added in 2B

    indexnotmatched := -1 //since indices all start from 0 

    for index := 0; index < len(args.Entries); index++ {

        if len(rf.Log) < (args.PrevLogIndex + index + 2){
          
            indexnotmatched = index

            rf.Log = rf.Log[:(args.PrevLogIndex + indexnotmatched + 1)]
            rf.Log = append(rf.Log, args.Entries[indexnotmatched:]...)  //... is used to append the rest of the entries that are already in the log
           
            break
        }


		if rf.Log[(args.PrevLogIndex + index + 1)].Term != args.Entries[index].Term{
		
            indexnotmatched = index
            //fmt.Println(indexnotmatched)
            rf.Log = rf.Log[:(args.PrevLogIndex + 1 + indexnotmatched)]
            rf.Log = append(rf.Log, args.Entries[indexnotmatched:]...)  //... is used to append the rest of the entries that are already in the log
            
            break
		}
    }
    rf.persist()

    //If leaderCommit > commitIndex, 
    //set commitIndex = min(leaderCommit, index of last new entry)

    if args.LeaderCommit > rf.CommitIndex {

		
		leadercommitindex := -1
		if args.LeaderCommit < len(rf.Log) - 1{
			leadercommitindex = args.LeaderCommit
		}else{
			leadercommitindex = len(rf.Log) - 1
		}


        rf.CommitIndex = leadercommitindex
    
        if rf.CommitIndex > rf.LastApplied {

            mylastappliedindex := rf.LastApplied
            mycommitindex := rf.CommitIndex

            //fmt.Println(mylastappliedindex,mycommitindex)
            entriesToApply := append([]LogEntry{}, rf.Log[(mylastappliedindex + 1):(mycommitindex + 1)]...)


            go rf.applyEntryRPC(mylastappliedindex + 1, entriesToApply)
        }
    }

   
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
    ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
    return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
    ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
    return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
    index := -1
    term := -1
    isLeader := true

    // Your code here (2B).
	//Added for 2B
    rf.mu.Lock()
    defer rf.mu.Unlock()

    term = rf.CurrentTerm
	isLeader = false

	if rf.State == State_Leader{

		isLeader = true

        //adding empty log in the leader
        rf.Log = append(rf.Log, LogEntry{Command: command, Term: term})

        rf.persist()
        index = len(rf.Log) - 1

        rf.MatchIndex[rf.me] = index 
        rf.NextIndex[rf.me] = index + 1
	}
    //fmt.Println("index, term, rf.me",index, term, rf.me)

    return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
    atomic.StoreInt32(&rf.dead, 1)
    // Your code here, if desired.
}
func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker(){
    // Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
    for rf.killed() == false{
        select {
        //case hbt := <- rf.HeartbeatTime.C:
        case <- rf.HeartbeatTime.C:
            rf.mu.Lock()

            //fmt.Println("heartbeatticker=",hbt)
            if rf.State == State_Leader {
                
                for peerid := 0; peerid < len(rf.peers); peerid++ {
                    if peerid != rf.me {
                        //fmt.Println("sendingAppendEntriesBroadcast for peerid= for currentstate",peerid, rf.State)
                        go rf.sendinitialAppendEntries(peerid)
                    }
                }

                //fmt.Println(reset heart beat)
                resetHeartbeatTime := 130 * time.Millisecond 
                rf.HeartbeatTime.Reset(resetHeartbeatTime)

            }
            rf.mu.Unlock()
        
        //case et := <- rf.ElectionTime.C:
        case <- rf.ElectionTime.C:
            rf.mu.Lock()
            //fmt.Println("ElectionTime=", et)

            mycurrentstate := rf.State 

            //fmt.Println("mycurrent state=",mycurrentstate)
            if mycurrentstate == State_Follower{
                 //fmt.Println("Inside follower")
                if rf.State != State_Candidate{
                   
                    
                    rf.State = State_Candidate
                    rf.CurrentTerm++

                    rf.VotedFor = rf.me 
                    rf.persist()
                    rf.VoteCount = 1

                    //fmt.Println("Convert to candidate")

                    electionTimeReset := time.Duration(130 + rand.Intn(130)*3) * time.Millisecond
                    //fmt.Println("electionTimeReset=", electionTimeReset)
                    rf.ElectionTime.Reset(electionTimeReset)

                    for peerid := 0; peerid < len(rf.peers); peerid++ {

                        if peerid != rf.me {
                            //fmt.Println("sendingAppendEntriesBroadcast for peerid= for currentstate",peerid, mycurrentstate)
                            go rf.sendAppendEntriesBroadcast(peerid)
                        }
                    }
                }
            }
            if mycurrentstate == State_Candidate{
                 //fmt.Println("Inside candidate")
                rf.CurrentTerm += 1

                rf.VotedFor = rf.me 
                rf.persist()
                rf.VoteCount = 1

                //Reset stops a ticker and resets its period to the specified duration. 
                //The next tick will arrive after the new period elapses. The duration d must be greater than zero; if not, Reset will panic.
                electionTimeReset := time.Duration(130 + rand.Intn(130)*3) * time.Millisecond
                //fmt.Println("electionTimeReset=", electionTimeReset)
                rf.ElectionTime.Reset(electionTimeReset)

                for peerid := 0; peerid < len(rf.peers); peerid++ {
                    if peerid != rf.me {
                        //fmt.Println("sendingAppendEntriesBroadcast for peerid= for currentstate",peerid, mycurrentstate)
                        go rf.sendAppendEntriesBroadcast(peerid)
                    }
                }
            }
            rf.mu.Unlock()
        }
    }
}
//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
    persister *Persister, applyCh chan ApplyMsg) *Raft {
    
    rf := &Raft{}

   
    rf.peers = peers
    rf.persister = persister
    rf.me = me

    // Your initialization code here (2A, 2B, 2C).
    rf.VotedFor = -1
    rf.Log = make([]LogEntry, 1) // start from index 1
    
    //create empty next index and match index of size of number of peers
    rf.NextIndex = make([]int, len(rf.peers))
    rf.MatchIndex = make([]int, len(rf.peers))

    //all states are initialzed to follower
    rf.State = State_Follower

    rf.ApplyCh = applyCh //applyCh is of chan ApplyMsg type
    
    //create initial tickers
    checkHeartbeatTime := 130 * time.Millisecond
    //fmt.Println("checkHeartbeatTime=",checkHeartbeatTime)
    rf.HeartbeatTime = time.NewTimer(checkHeartbeatTime)
    checkNow := time.Duration(rand.Intn(130)*3 + 130) * time.Millisecond
    //fmt.Println("checkNow=",checkNow)
    rf.ElectionTime = time.NewTimer(checkNow)

    // initialize from state persisted before a crash
    
    rf.readPersist(persister.ReadRaftState())
    
    for index := 0; index < len(rf.NextIndex); index++{
        rf.NextIndex[index] = len(rf.Log)
    }

    for index := 0; index < len(rf.MatchIndex); index++{
        rf.MatchIndex[index] = len(rf.Log) - 1
    }
    // start ticker goroutine to start elections
    go rf.ticker()


    

    return rf
}






