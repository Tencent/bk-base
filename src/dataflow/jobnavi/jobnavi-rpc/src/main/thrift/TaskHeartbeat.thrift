namespace java com.tencent.bk.base.dataflow.jobnavi.rpc.runner
namespace py task_heartbeat

struct TaskLauncherInfo {
    1: i64 executeId;
    2: i32 port;
}

struct ProcessResult{
    1: bool success;
    2: string processInfo;
}


service TaskHeartBeatService {
    ProcessResult heartbeat(1: TaskLauncherInfo info);
    ProcessResult add(1: TaskLauncherInfo info);
}
