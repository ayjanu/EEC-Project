#ifndef Scheduler_hpp
#define Scheduler_hpp

#include <vector>
#include <map>
#include <set>
#include <algorithm>

#include "Interfaces.h"
#include "SimTypes.h"

/**
 * A basic, load-aware scheduler:
 *  - Prefills 50 VMs on initialization.
 *  - Picks the VM with the fewest tasks (lowest "load") that meets
 *    CPU/memory constraints.
 *  - If SLA0 can't find a suitable lightly-loaded VM, we create a new VM.
 */
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

    // SafeRemoveTask for forcibly removing a task from a VM (if needed)
    bool SafeRemoveTask(VMId_t vm, TaskId_t task);

    // Expose some data structures if needed
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

    // Stub methods for advanced operations
    void ConsolidateVMs(Time_t now);
    MachineId_t FindMigrationTarget(VMId_t vm, Time_t now);

    // -------------- Internal Data Structures --------------
    // We'll keep vmStack for the 50 prefilled VMs, though we
    // do not rely on stack-based searching in "NewTask()" anymore.
    std::vector<VMId_t>         vmStack;
    std::vector<VMId_t>         vms;
    std::vector<MachineId_t>    machines;
    std::vector<MachineId_t>    sortedMachinesByEfficiency;
    std::set<MachineId_t>       activeMachines;

    // Key: tracks the last known utilization of each machine
    std::map<MachineId_t, double> machineUtilization;

    // Simple migration tracking (if needed)
    std::map<VMId_t, MachineId_t> pendingMigrations;
    std::map<VMId_t, Time_t>      lastMigrationTime;
    const Time_t                  MIGRATION_COOLDOWN = 1000000; // 1 second

    // Constants for possible P-state logic
    const double UNDERLOAD_THRESHOLD = 0.3;
    const double OVERLOAD_THRESHOLD  = 0.8;
};

#endif /* Scheduler_hpp */
