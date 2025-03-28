#include "Scheduler.hpp"

#include <string>
#include <chrono>
#include <iomanip>
#include <sstream>
#include <iostream>
#include <climits>
#include <algorithm>
#include <utility>
#include "Internal_Interfaces.h"

// ----------------------------------------------------------------
//  GLOBAL SCHEDULER INSTANCE
// ----------------------------------------------------------------
static Scheduler scheduler;

/**
 * Helper function to check if a machine has SLA0 or SLA1 tasks.
 * We use it in PeriodicCheck() to decide if we want to run at high performance (P0).
 */
static bool HasHighPriorityTasks(MachineId_t machine_id) {
    for (VMId_t vm : scheduler.GetVMs()) {
        try {
            VMInfo_t vmInfo = VM_GetInfo(vm);
            if (vmInfo.machine_id != machine_id) {
                continue;
            }
            // If migrating, we could skip, but not handled in this simple code
            for (TaskId_t task : vmInfo.active_tasks) {
                SLAType_t slaType = RequiredSLA(task);
                if (slaType == SLA0 || slaType == SLA1) {
                    return true;
                }
            }
        } catch (...) {
            // ignore any errors
        }
    }
    return false;
}

/**
 * Helper function to measure the "load" on a VM.
 * We define load = number of active tasks in that VM.
 */
static unsigned GetVMLoad(VMId_t vm_id) {
    try {
        VMInfo_t info = VM_GetInfo(vm_id);
        return (unsigned)info.active_tasks.size();
    } catch (...) {
        // If we fail for some reason, treat as high load
        return UINT_MAX;
    }
}

// ------------------- Scheduler Methods ------------------- //

void Scheduler::Init() {
    unsigned totalMachines = Machine_GetTotal();
    std::map<CPUType_t, std::vector<MachineId_t>> machinesByCPU;
    std::vector<std::pair<unsigned, MachineId_t>> machineEfficiencies;

    for (unsigned i = 0; i < totalMachines; i++) {
        MachineId_t machineId = MachineId_t(i);
        machines.push_back(machineId);

        try {
            MachineInfo_t info = Machine_GetInfo(machineId);
            // Group machines by CPU type
            machinesByCPU[info.cpu].push_back(machineId);

            // Grab S0 power usage if possible
            if (!info.s_states.empty() && info.s_states.size() > S0) {
                machineEfficiencies.push_back({info.s_states[S0], machineId});
            } else {
                machineEfficiencies.push_back({UINT_MAX, machineId});
            }

            // Track as active if S0
            if (info.s_state == S0) {
                activeMachines.insert(machineId);
                machineUtilization[machineId] = 0.0;
            } else {
                machineUtilization[machineId] = 0.0;
            }
        } catch (...) {
            SimOutput("Init: Error retrieving machine info for ID=" +
                      std::to_string(i), 1);
        }
    }

    // Sort machines by ascending S0 power usage => sortedMachinesByEfficiency
    std::sort(machineEfficiencies.begin(), machineEfficiencies.end(),
              [](auto &a, auto &b){ return a.first < b.first; });
    for (auto &pair : machineEfficiencies) {
        sortedMachinesByEfficiency.push_back(pair.second);
    }

    // ---------------------------------------------------------------
    //  Prefill the stack with at least 50 VMs (as a starting point).
    // ---------------------------------------------------------------
    unsigned desired_prefill = 50;
    unsigned created = 0;
    for (MachineId_t mach : sortedMachinesByEfficiency) {
        if (created >= desired_prefill) {
            break;
        }
        try {
            MachineInfo_t minfo = Machine_GetInfo(mach);
            // We only attach if machine is up (S0) and has memory for overhead
            if (minfo.s_state == S0 && minfo.memory_size >= VM_MEMORY_OVERHEAD) {
                // Create a generic LINUX VM using the machine’s CPU type
                VMId_t vm = VM_Create(LINUX, minfo.cpu);
                VM_Attach(vm, mach);

                // Push onto stack, track in vms
                vmStack.push_back(vm);
                vms.push_back(vm);
                created++;
            }
        } catch (...) {
            // If creation/attachment fails, ignore
        }
    }

    SimOutput("Init: Scheduler (load-aware) initialized, prefilled " +
              std::to_string(created) + " VMs.", 2);
}


void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
    // Gather basic info about the incoming task
    CPUType_t required_cpu = RequiredCPUType(task_id);
    VMType_t  required_vm  = RequiredVMType(task_id);
    SLAType_t sla_type     = RequiredSLA(task_id);
    unsigned  required_mem = GetTaskMemory(task_id);

    // Priority logic (same as before)
    TaskInfo_t tinfo = GetTaskInfo(task_id);
    bool urgent = false;
    if (tinfo.target_completion > 0 && (tinfo.target_completion - now <= 12000000)) {
        urgent = true;
    }

    Priority_t priority;
    switch (sla_type) {
        case SLA0: priority = HIGH_PRIORITY; break;
        case SLA1: priority = MID_PRIORITY;  break;
        case SLA2: priority = LOW_PRIORITY;  break;
        case SLA3:
        default:   priority = LOW_PRIORITY;  break;
    }
    if (urgent) {
        priority = HIGH_PRIORITY;
    }

    // ----------------------------------------------------
    //  Step 1: Try to find the existing VM with the least load
    //          that meets CPU/memory constraints.
    // ----------------------------------------------------
    VMId_t target_vm = VMId_t(-1);
    unsigned minLoad = UINT_MAX;

    // We'll iterate over all known VMs to find the best match.
    for (VMId_t vm : vms) {
        // Skip if VM is migrating
        if (pendingMigrations.find(vm) != pendingMigrations.end()) {
            continue;
        }
        try {
            VMInfo_t vmInfo = VM_GetInfo(vm);
            // Must match required CPU & VM type
            if (vmInfo.cpu != required_cpu) {
                continue;
            }
            if (vmInfo.vm_type != required_vm) {
                continue;
            }

            MachineInfo_t machInfo = Machine_GetInfo(vmInfo.machine_id);
            if (machInfo.s_state != S0) {
                continue; // only place tasks on S0 machines
            }
            // Check memory
            if (machInfo.memory_used + required_mem > machInfo.memory_size) {
                continue;
            }

            // Check load (number of tasks on this VM)
            unsigned load = (unsigned)vmInfo.active_tasks.size();
            // If we found a VM with a lower load, pick it
            if (load < minLoad) {
                minLoad   = load;
                target_vm = vm;
            }
        } catch (...) {
            // skip errors
        }
    }

    // ----------------------------------------------------
    //  Step 2: If SLA0 and no suitable VM found OR if all are
    //          heavily loaded, create a new VM right away.
    // ----------------------------------------------------
    bool createFreshVM = false;
    if (sla_type == SLA0 && target_vm == VMId_t(-1)) {
        // For SLA0 tasks, if no suitable lightly-loaded VM, we always create a new VM
        createFreshVM = true;
    }

    // Optionally, we could decide: if minLoad is above some threshold, create a new VM.
    // e.g., if (minLoad >= 5) createFreshVM = true;   // Just an example threshold
    // We'll leave it up to you to refine. 
    // For now, we only do it for SLA0 if not found.

    // If we can't find a good VM for SLA0 or none was found for any SLA...
    if (target_vm == VMId_t(-1) || createFreshVM) {
        // Look for an S0 machine with enough memory
        MachineId_t chosen_machine = MachineId_t(-1);
        for (MachineId_t m : sortedMachinesByEfficiency) {
            try {
                MachineInfo_t info = Machine_GetInfo(m);
                if (info.s_state == S0 &&
                    info.cpu == required_cpu && 
                    (info.memory_used + required_mem + VM_MEMORY_OVERHEAD <= info.memory_size)) {
                    chosen_machine = m;
                    break;
                }
            } catch (...) {
                // ignore
            }
        }

        if (chosen_machine != MachineId_t(-1)) {
            try {
                VMId_t newVm = VM_Create(required_vm, required_cpu);
                VM_Attach(newVm, chosen_machine);
                vms.push_back(newVm);
                // We can also push to vmStack if desired
                vmStack.push_back(newVm);
                target_vm = newVm;
            } catch (...) {
                SimOutput("NewTask: Could not create VM for task " +
                          std::to_string(task_id), 2);
                target_vm = VMId_t(-1);
            }
        }
    }

    // -------------------------------------------------
    //  Step 3: If we have a VM, add the task to it
    // -------------------------------------------------
    if (target_vm != VMId_t(-1)) {
        try {
            VMInfo_t vmInfo = VM_GetInfo(target_vm);
            MachineInfo_t machInfo = Machine_GetInfo(vmInfo.machine_id);
            if (machInfo.s_state == S0 &&
                (machInfo.memory_used + required_mem <= machInfo.memory_size)) {
                VM_AddTask(target_vm, task_id, priority);
            } else {
                MemoryWarning(now, machInfo.machine_id);
            }
        } catch (...) {
            SimOutput("NewTask: Could not add task " + std::to_string(task_id) +
                      " to VM " + std::to_string(target_vm), 2);
        }
    } else {
        SimOutput("NewTask: No suitable VM found for task " +
                  std::to_string(task_id), 2);
    }
}


void Scheduler::PeriodicCheck(Time_t now) {
    // Update machine-level utilization
    for (MachineId_t machine : machines) {
        if (activeMachines.count(machine)) {
            try {
                MachineInfo_t info = Machine_GetInfo(machine);
                if (info.s_state == S0) {
                    double utilization =
                        (info.num_cpus > 0)
                        ? double(info.active_tasks) / double(info.num_cpus)
                        : 0.0;
                    machineUtilization[machine] = utilization;
                } else {
                    machineUtilization[machine] = 0.0;
                }
            } catch (...) {
                activeMachines.erase(machine);
                machineUtilization[machine] = 0.0;
            }
        } else {
            machineUtilization[machine] = 0.0;
        }
    }

    // Simple p-state logic
    for (MachineId_t machine : activeMachines) {
        try {
            MachineInfo_t info = Machine_GetInfo(machine);
            if (info.s_state != S0) {
                continue;
            }

            CPUPerformance_t targetPState = P3;
            bool highPri = HasHighPriorityTasks(machine);

            if (highPri) {
                targetPState = P0;  
            } else if (info.active_tasks > 0) {
                double util = machineUtilization[machine];
                if      (util > 0.75) targetPState = P0;
                else if (util > 0.3)  targetPState = P1;
                else                  targetPState = P2;
            }

            if (info.p_state != targetPState) {
                Machine_SetCorePerformance(machine, 0, targetPState);
            }
        } catch (...) {
            // ignore
        }
    }
}

void Scheduler::TaskComplete(Time_t now, TaskId_t task_id) {
    // We simply log it
    SimOutput("TaskComplete: Task " + std::to_string(task_id) +
              " finished at time " + std::to_string(now), 3);
}

void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id) {
    // If we had a record of a migration, remove it
    auto it = pendingMigrations.find(vm_id);
    if (it != pendingMigrations.end()) {
        pendingMigrations.erase(it);
    }
    MigrationDone(time, vm_id); // Inform external callback
}

bool Scheduler::SafeRemoveTask(VMId_t vm, TaskId_t task) {
    try {
        VM_RemoveTask(vm, task);
        return true;
    } catch (const std::runtime_error& e) {
        SimOutput("SafeRemoveTask: " + std::string(e.what()), 2);
    } catch (...) {
        SimOutput("SafeRemoveTask: Unknown error removing task.", 2);
    }
    return false;
}

void Scheduler::ConsolidateVMs(Time_t now) {
    // Stub – no consolidation logic in this demonstration
}

MachineId_t Scheduler::FindMigrationTarget(VMId_t vm, Time_t now) {
    // Stub – no advanced migration logic
    return MachineId_t(-1);
}

void Scheduler::Shutdown(Time_t time) {
    SimOutput("Shutdown: Stopping VMs.", 3);
    for (VMId_t vm : vms) {
        try {
            VMInfo_t vmInfo = VM_GetInfo(vm);
            if (vmInfo.machine_id != MachineId_t(-1)) {
                VM_Shutdown(vm);
            }
        } catch (...) {
            SimOutput("Shutdown: Could not shut down VM " + std::to_string(vm), 2);
        }
    }
    SimOutput("SimulationComplete(): Time is " + std::to_string(time), 0);
}

// --------------- C-Style Hooks --------------- //

void InitScheduler() {
    SimOutput("InitScheduler() starting", 1);
    scheduler.Init();
    SimOutput("InitScheduler() finished", 1);
}

void HandleNewTask(Time_t time, TaskId_t task_id) {
    scheduler.NewTask(time, task_id);
}

void HandleTaskCompletion(Time_t time, TaskId_t task_id) {
    scheduler.TaskComplete(time, task_id);
}

void MemoryWarning(Time_t time, MachineId_t machine_id) {
    SimOutput("MemoryWarning: Overcommit on machine " + std::to_string(machine_id) +
              " at time " + std::to_string(time), 1);
}

void SchedulerCheck(Time_t time) {
    scheduler.PeriodicCheck(time);
}

void MigrationDone(Time_t time, VMId_t vm_id) {
    SimOutput("MigrationDone: VM " + std::to_string(vm_id) +
              " at time " + std::to_string(time), 3);
}

void SimulationComplete(Time_t time) {
    // Basic summary
    std::cout << "SLA violation report:" << std::endl;
    std::cout << "  SLA0: " << GetSLAReport(SLA0) << "%" << std::endl;
    std::cout << "  SLA1: " << GetSLAReport(SLA1) << "%" << std::endl;
    std::cout << "  SLA2: " << GetSLAReport(SLA2) << "%" << std::endl;
    std::cout << "  SLA3: " << GetSLAReport(SLA3) << "%" << std::endl;
    std::cout << "Total Energy: " << Machine_GetClusterEnergy() << " KW-Hour" << std::endl;
    std::cout << "Finished in " << double(time)/1000000.0 << " seconds" << std::endl;

    SimOutput("SimulationComplete(): Final reporting done.", 0);
    scheduler.Shutdown(time);
}

void StateChangeComplete(Time_t time, MachineId_t machine_id) {
    // Not used in this example
}

void SLAWarning(Time_t time, TaskId_t task_id) {
    // Not used in this example
}
