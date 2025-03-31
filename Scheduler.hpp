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
    Scheduler() {}

    // --- Core interface methods invoked by the simulator ---
    void Init();
    void NewTask(Time_t now, TaskId_t task_id);
    void TaskComplete(Time_t now, TaskId_t task_id);
    void PeriodicCheck(Time_t now);
    void MigrationComplete(Time_t time, VMId_t vm_id);
    void Shutdown(Time_t time);

    bool SafeRemoveTask(VMId_t vm, TaskId_t task);

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
        machineUtilization[machine] = 0.0;
    }

    void ConsolidateVMs(Time_t now);
    MachineId_t FindMigrationTarget(VMId_t vm, Time_t now);

    // -------------- Internal Data Structures --------------
    std::vector<VMId_t>         vmStack;
    std::vector<VMId_t>         vms;
    std::vector<MachineId_t>    machines;
    std::set<MachineId_t>       activeMachines;

    std::map<MachineId_t, double> machineUtilization;

    std::map<VMId_t, MachineId_t> pendingMigrations;
    std::map<VMId_t, Time_t>      lastMigrationTime;
    const Time_t                  MIGRATION_COOLDOWN = 1000000; // 1 second

    // Constants for possible P-state logic
    const double UNDERLOAD_THRESHOLD = 0.3;
    const double OVERLOAD_THRESHOLD  = 0.8;
};

#endif /* Scheduler_hpp */