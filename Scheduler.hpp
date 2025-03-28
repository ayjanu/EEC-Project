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

#include "Interfaces.h"
#include "SimTypes.h"

class Scheduler {
public:
    std::map<MachineId_t, std::map<unsigned, CPUPerformance_t>> cpuStates;
    std::map<TaskId_t, Time_t> taskStartTimes;
    std::map<TaskId_t, SLAType_t> taskSLAs;
    const std::vector<VMId_t>& GetVMs() const { return vms; }
    const std::vector<MachineId_t>& GetMachines() const { return machines; }
    bool IsMachineActive(MachineId_t machine) const { 
        return activeMachines.find(machine) != activeMachines.end(); 
    }
    bool SafeRemoveTask(VMId_t vm, TaskId_t task);
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
    void ConsolidateVMs(Time_t now);
    
    Scheduler() {}
    void Init();
    void MigrationComplete(Time_t time, VMId_t vm_id);
    void NewTask(Time_t now, TaskId_t task_id);
    void PeriodicCheck(Time_t now);
    void Shutdown(Time_t time);
    void TaskComplete(Time_t now, TaskId_t task_id);
    
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
};

#endif /* Scheduler_hpp */