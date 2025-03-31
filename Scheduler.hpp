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
    const std::vector<VMId_t>& GetVMs() const { return vms; }
    const std::vector<MachineId_t>& GetMachines() const { return machines; }
    bool IsMachineActive(MachineId_t machine) const { 
        return activeMachines.find(machine) != activeMachines.end(); 
    }
    void ActivateMachine(MachineId_t machine) {
        activeMachines.insert(machine);
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
    
    // Thresholds for underload/overload detection
    const double UNDERLOAD_THRESHOLD = 0.3;  // 30% utilization
    const double OVERLOAD_THRESHOLD = 0.8;   // 80% utilization
    

    
    // Track which machines are powered on
    std::set<MachineId_t> activeMachines;
    std::set<TaskId_t> pendingTasks;
    
    // Lists of VMs and machines
    std::vector<VMId_t> vms;
    std::vector<MachineId_t> machines;

    std::map<CPUType_t, std::set<MachineId_t>> machinesByCPU;
    std::map<VMType_t, std::set<VMId_t>> vmsByType;
    std::map<MachineId_t, std::set<VMId_t>> vmsByMachine;

    std::map<VMId_t, MachineId_t> pendingMigrations;
    std::map<VMId_t, Time_t> lastMigrationTime; // Track last migration time per VM
    const Time_t MIGRATION_COOLDOWN = 1000000; // 1 second cooldown


};

#endif /* Scheduler_hpp */