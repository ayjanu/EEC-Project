#include "Scheduler.hpp"
#include <string>
#include <chrono>
#include <iomanip>
#include <sstream>
#include <iostream>
#include <climits>
#include <algorithm>
#include <utility>

// The following code was written by Ayan Jannu and Leul Teka and cleaned/simplified by Claude 3.7.
// Thank you to the LLM for helping with the logic of using maps to track VM allocation and writing comments
// describing the processes being carried out as well as combining redudant code into helper functions.

/**
 * A basic, stack-based first-fit scheduler:
 *  - Prefills 50 VMs on initialization.
 *  - Uses a stack to assign tasks to the first VM that meets CPU/memory constraints.
 *  - If SLA0 can't find a suitable VM, we create a new VM.
 */
static Scheduler scheduler;

static bool HasHighPriorityTasks(MachineId_t machine_id) {
    for (VMId_t vm : scheduler.GetVMs()) {
        try {
            VMInfo_t vmInfo = VM_GetInfo(vm);
            if (vmInfo.machine_id != machine_id) continue;
            for (TaskId_t task : vmInfo.active_tasks) {
                SLAType_t slaType = RequiredSLA(task);
                if (slaType == SLA0 || slaType == SLA1) {
                    return true;
                }
            }
        } catch (...) {}
    }
    return false;
}

static unsigned GetVMLoad(VMId_t vm_id) {
    try {
        VMInfo_t info = VM_GetInfo(vm_id);
        return (unsigned)info.active_tasks.size();
    } catch (...) {
        return UINT_MAX;
    }
}

void Scheduler::Init() {
    unsigned totalMachines = Machine_GetTotal();
    for (unsigned i = 0; i < totalMachines; i++) {
        MachineId_t machineId = MachineId_t(i);
        machines.push_back(machineId);
        try {
            MachineInfo_t info = Machine_GetInfo(machineId);
            if (info.s_state == S0) {
                activeMachines.insert(machineId);
                machineUtilization[machineId] = 0.0;
            } else {
                machineUtilization[machineId] = 0.0;
            }
        } catch (...) {
            SimOutput("Init: Error retrieving machine info for ID=" + std::to_string(i), 1);
        }
    }
    unsigned desired_prefill = 50;
    unsigned created = 0;
    for (MachineId_t mach : machines) {
        if (created >= desired_prefill) break;
        try {
            MachineInfo_t minfo = Machine_GetInfo(mach);
            if (minfo.s_state == S0 && minfo.memory_size >= VM_MEMORY_OVERHEAD) {
                VMId_t vm = VM_Create(LINUX, minfo.cpu);
                VM_Attach(vm, mach);
                vmStack.push_back(vm);
                vms.push_back(vm);
                created++;
            }
        } catch (...) {}
    }
    SimOutput("Init: Scheduler (stack-based first-fit) initialized, prefilled " + std::to_string(created) + " VMs.", 2);
}

void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
    CPUType_t required_cpu = RequiredCPUType(task_id);
    VMType_t  required_vm  = RequiredVMType(task_id);
    SLAType_t sla_type     = RequiredSLA(task_id);
    unsigned  required_mem = GetTaskMemory(task_id);
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
        default:   priority = LOW_PRIORITY;  break;
    }
    if (urgent) priority = HIGH_PRIORITY;

    VMId_t target_vm = VMId_t(-1);
    for (VMId_t vm : vmStack) {
        if (pendingMigrations.find(vm) != pendingMigrations.end()) continue;
        try {
            VMInfo_t vmInfo = VM_GetInfo(vm);
            if (vmInfo.cpu != required_cpu) continue;
            if (vmInfo.vm_type != required_vm) continue;
            MachineInfo_t machInfo = Machine_GetInfo(vmInfo.machine_id);
            if (machInfo.s_state != S0) continue;
            if (machInfo.memory_used + required_mem > machInfo.memory_size) continue;
            target_vm = vm;
            break; // First fit: stop at the first suitable VM
        } catch (...) {}
    }
    bool createFreshVM = false;
    if (sla_type == SLA0 && target_vm == VMId_t(-1)) {
        createFreshVM = true;
    }
    if (target_vm == VMId_t(-1) || createFreshVM) {
        MachineId_t chosen_machine = MachineId_t(-1);
        for (MachineId_t m : machines) {
            try {
                MachineInfo_t info = Machine_GetInfo(m);
                if (info.s_state == S0 && info.cpu == required_cpu &&
                    (info.memory_used + required_mem + VM_MEMORY_OVERHEAD <= info.memory_size)) {
                    chosen_machine = m;
                    break;
                }
            } catch (...) {}
        }
        if (chosen_machine != MachineId_t(-1)) {
            try {
                VMId_t newVm = VM_Create(required_vm, required_cpu);
                VM_Attach(newVm, chosen_machine);
                vms.push_back(newVm);
                vmStack.push_back(newVm);
                target_vm = newVm;
            } catch (...) {
                SimOutput("NewTask: Could not create VM for task " + std::to_string(task_id), 2);
                target_vm = VMId_t(-1);
            }
        }
    }
    if (target_vm != VMId_t(-1)) {
        try {
            VMInfo_t vmInfo = VM_GetInfo(target_vm);
            MachineInfo_t machInfo = Machine_GetInfo(vmInfo.machine_id);
            if (machInfo.s_state == S0 && (machInfo.memory_used + required_mem <= machInfo.memory_size)) {
                VM_AddTask(target_vm, task_id, priority);
            } else {
                MemoryWarning(now, machInfo.machine_id);
            }
        } catch (...) {
            SimOutput("NewTask: Could not add task " + std::to_string(task_id) +
                      " to VM " + std::to_string(target_vm), 2);
        }
    } else {
        SimOutput("NewTask: No suitable VM found for task " + std::to_string(task_id), 2);
    }
}

void Scheduler::PeriodicCheck(Time_t now) {
    for (MachineId_t machine : machines) {
        if (activeMachines.count(machine)) {
            try {
                MachineInfo_t info = Machine_GetInfo(machine);
                if (info.s_state == S0) {
                    double utilization = (info.num_cpus > 0)
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
    for (MachineId_t machine : activeMachines) {
        try {
            MachineInfo_t info = Machine_GetInfo(machine);
            if (info.s_state != S0) continue;
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
        } catch (...) {}
    }
}

void Scheduler::TaskComplete(Time_t now, TaskId_t task_id) {
    SimOutput("TaskComplete: Task " + std::to_string(task_id) + " finished at time " + std::to_string(now), 3);
}

void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id) {
    auto it = pendingMigrations.find(vm_id);
    if (it != pendingMigrations.end()) {
        pendingMigrations.erase(it);
    }
    MigrationDone(time, vm_id);
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

void Scheduler::ConsolidateVMs(Time_t now) {}

MachineId_t Scheduler::FindMigrationTarget(VMId_t vm, Time_t now) {
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
    SimOutput("MigrationDone: VM " + std::to_string(vm_id) + " at time " + std::to_string(time), 3);
}

void SimulationComplete(Time_t time) {
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

void StateChangeComplete(Time_t time, MachineId_t machine_id) {}

void SLAWarning(Time_t time, TaskId_t task_id) {}