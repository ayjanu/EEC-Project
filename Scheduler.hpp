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
    // Existing methods and members...
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
    
    // New method for migration target selection
    MachineId_t FindMigrationTarget(VMId_t vm, Time_t now);

    // Thresholds
    const double UNDERLOAD_THRESHOLD = 0.3;
    const double OVERLOAD_THRESHOLD = 0.8;
    
    // Tracking
    std::map<MachineId_t, double> machineUtilization;
    std::set<MachineId_t> activeMachines;
    std::vector<VMId_t> vms;
    std::vector<MachineId_t> machines;
    std::vector<MachineId_t> sortedMachinesByEfficiency;
    
    // Track pending migrations (VM ID -> Target Machine ID)
    std::map<VMId_t, MachineId_t> pendingMigrations;
    std::map<VMId_t, Time_t> lastMigrationTime; // Track last migration time per VM
    const Time_t MIGRATION_COOLDOWN = 1000000; // 1 second cooldown

};

#endif /* Scheduler_hpp */