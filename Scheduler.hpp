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
    void Init();
    void NewTask(Time_t now, TaskId_t task_id);
    void PeriodicCheck(Time_t now);
    void TaskComplete(Time_t now, TaskId_t task_id);
    void Shutdown(Time_t time);
    void MigrationComplete(Time_t time, VMId_t vm_id);
    void HandleMemoryWarning(Time_t time, MachineId_t machine_id);
    void HandleSLAWarning(Time_t time, TaskId_t task_id);
    void HandleStateChangeComplete(Time_t time, MachineId_t machine_id);
    
    const std::vector<VMId_t>& GetVMs() const { return vms; }
    
private:
    std::vector<MachineId_t> machines;
    std::vector<VMId_t> vms;
    std::set<MachineId_t> activeMachines;    // Machines in S0 state, ready to run tasks
    std::set<MachineId_t> standbyMachines;   // Machines in S1/S2 state, can be activated quickly
    std::set<MachineId_t> poweredOffMachines; // Machines in S3/S4/S5 state, need more time to activate
    std::map<MachineId_t, double> machineUtilization;
    std::map<MachineId_t, MachineState_t> machineStates;
    std::map<MachineId_t, bool> stateChangeInProgress;
    
    MachineId_t findLeastLoadedMachine(CPUType_t cpuType) const;
    void activateMachine(Time_t time, MachineId_t machine);
    void standbyMachine(Time_t time, MachineId_t machine);
    void powerOffMachine(Time_t time, MachineId_t machine);
    bool needsMoreCapacity() const;
};


#endif /* Scheduler_hpp */