package mr

type TaskKind int

const (
	MapTask TaskKind = iota
	ReduceTask
	WaitTask
	ExitTask
)

type TaskRequest struct{}

type TaskReply struct {
	MapAttempts []int
	Attempt     int
	Kind        TaskKind
	ID          int
	File        string
	NMap        int
	NReduce     int
}

type TaskComplete struct {
	Output  string
	Attempt int
	Kind    TaskKind
	ID      int
}

type CompleteReply struct{}
