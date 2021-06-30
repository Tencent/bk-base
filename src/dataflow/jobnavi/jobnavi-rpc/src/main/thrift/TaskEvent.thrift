namespace java com.tencent.bk.base.dataflow.jobnavi.rpc
namespace py task_event

struct TaskEvent {
    1: EventContext context;
    2: TaskStatus changeStatus;
    3: string eventName;
}

struct EventContext{
    1: ExecuteInfo executeInfo;
    2: TaskInfo taskInfo;
    3: string eventInfo;
}

struct ExecuteInfo{
    1: required i64 id;
    2: string host;
    3: double rank;
}

struct TaskInfo{
    1: required string scheduleId;
    2: required i64 scheduleTime;
    3: TaskType type;
    4: optional string extraInfo;
    5: TaskRecoveryInfo recoveryInfo;
    6: string decommissionTimeout;
    7: string nodeLabel;
    8: i64 dataTime;
}

enum Language {
    java, python, go
}

enum TaskMode {
    thread, process
}

struct TaskType{
    1: required string name;
    2: required string main;
    3: optional string description;
    4: required string env;
    5: optional string sysEnv;
    6: optional Language language;
    7: required TaskMode taskMode = TaskMode.process;
    8: required string tag;
}

struct TaskRecoveryInfo{
    1: required bool recoveryEnable;
    2: i32 recoveryTimes;
    3: string intervalTime;
    4: i32 maxRecoveryTimes;
}

enum TaskStatus{
    none, preparing, running, finished, killed, failed, disabled, skipped, decommissioned, failed_succeeded, lost, recovering, retied
}

struct TaskEventResult{
    1: bool success;
    2: string processInfo;
    3: list<TaskEvent> nextEvents;
}

exception RequestException {
    1: required i32 code;
    2: optional string reason;
}


service TaskEventService {
    TaskEventResult processEvent(1: TaskEvent event) throws (1:RequestException qe);
}
