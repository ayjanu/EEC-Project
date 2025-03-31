//
//  Scheduler.hpp
//  CloudSim
//

#ifndef Scheduler_hpp
#define Scheduler_hpp

#include <vector>
#include <map>
#include <set>
#include <algorithm>
#include <queue>

#include "Interfaces.h"
#include "SimTypes.h"

// Custom comparator for sorting tasks by target completion time
struct TaskCompare {
    bool operator()(TaskId_t a, TaskId_t b) const {
        Time_t targetA = GetTaskInfo(a).target_completion;
        Time_t targetB = GetTaskInfo(b).target_completion;
        return targetA > targetB; // Sort in ascending order (earlier deadlines first)
    }
};

class Scheduler {
public:
    const std::vector<VMId_t>& GetVMs() const { return vms; }
    const std::vector<MachineId_t>& GetMachines() const { return machines; }
    bool IsMachineActive(MachineId_t machine) const { 
        return activeMachines.find(machine) != activeMachines.end(); 
    }
    void ActivateMachine(MachineId_t machine) {
        activeMachines.insert(machine);
        machineUtilization[machine] = 0.0;
    }
    void DeactivateMachine(MachineId_t machine) {
        activeMachines.erase(machine);
    }
    
    void AddVM(VMId_t vm) {
        vms.push_back(vm);
    }
    bool AssignTaskToVM(TaskId_t task_id, Time_t now);
    
    Scheduler() {}
    void Init();
    void MigrationComplete(Time_t time, VMId_t vm_id);
    void NewTask(Time_t now, TaskId_t task_id);
    void PeriodicCheck(Time_t now);
    void Shutdown(Time_t time);
    void TaskComplete(Time_t now, TaskId_t task_id);
    
    // Make these public so they can be accessed by the global functions
    std::map<VMType_t, std::set<VMId_t>> vmsByType;
    std::map<MachineId_t, std::set<VMId_t>> vmsByMachine;
    
private:
    // Thresholds for underload/overload detection
    const double UNDERLOAD_THRESHOLD = 0.3;  // 30% utilization
    const double OVERLOAD_THRESHOLD = 0.8;   // 80% utilization
    
    // Track machine utilization
    std::map<MachineId_t, double> machineUtilization;
    
    // Track which machines are powered on
    std::set<MachineId_t> activeMachines;
    
    // Lists of VMs and machines
    std::vector<VMId_t> vms;
    std::vector<MachineId_t> machines;

    std::map<CPUType_t, std::set<MachineId_t>> machinesByCPU;
    
    // Queue for incoming tasks
    std::queue<TaskId_t> pendingTasksQueue;
    
    // Priority queue for tasks sorted by target_completion
    std::priority_queue<TaskId_t, std::vector<TaskId_t>, TaskCompare> sortedPendingTasks;
};

#endif /* Scheduler_hpp */