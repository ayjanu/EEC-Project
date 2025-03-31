//
//  Scheduler.cpp
//  CloudSim
//

#include "Scheduler.hpp"
#include "Internal_Interfaces.h"
#include <string>
#include <iostream>
#include <climits>
#include <algorithm>

// The following code was written by Ayan Jannu and Leul Teka and simplified/cleaned and commented by Claude 3.7

// Global Scheduler instance
static Scheduler scheduler;

void Scheduler::Init() {
    unsigned totalMachines = Machine_GetTotal();
    std::map<CPUType_t, std::vector<MachineId_t>> machinesByCPU;
    
    // Gather all machines and organize by CPU type
    for (unsigned i = 0; i < totalMachines; i++) {
        MachineId_t machineId = MachineId_t(i);
        machines.push_back(machineId);
        MachineInfo_t info = Machine_GetInfo(machineId);
        CPUType_t cpuType = info.cpu;
        machinesByCPU[cpuType].push_back(machineId);
        for (unsigned i = 0; i < info.num_cpus; i++) {
            Machine_SetCorePerformance(machineId, i, P0);
        }
    }
    
    // Create 3 VMs per machine to handle the workload
    for (const auto &pair : machinesByCPU) {
        CPUType_t cpuType = pair.first;
        const std::vector<MachineId_t> &machinesWithCPU = pair.second;
        if (machinesWithCPU.empty()) {
            continue;
        }
        for (unsigned i = 0; i < machinesWithCPU.size(); i++) {
            VMId_t vm = VM_Create(LINUX, cpuType);
            VMId_t vm2 = VM_Create(LINUX, cpuType);
            VMId_t vm3 = VM_Create(LINUX, cpuType);
            vms.push_back(vm);
            vms.push_back(vm2);
            vms.push_back(vm3);
            MachineId_t machine = machinesWithCPU[i % machinesWithCPU.size()];
            VM_Attach(vm, machine);
            VM_Attach(vm2, machine);
            VM_Attach(vm3, machine);
            activeMachines.insert(machine);
            
            // Track utilization
            machineUtilization[machine] = 0.0;
        }
    }
    SimOutput("NVIDIA Scheduler initialized with " + std::to_string(activeMachines.size()), 0);
}

// Helper function to try assigning a task to a VM
bool Scheduler::AssignTaskToVM(TaskId_t task_id, Time_t now) {
    CPUType_t requiredCPU = RequiredCPUType(task_id);
    SLAType_t slaType = RequiredSLA(task_id);
    TaskInfo_t taskInfo = GetTaskInfo(task_id);

    // Determine task urgency and base priority
    bool urgent = (taskInfo.target_completion - static_cast<uint64_t>(now) <= 12000000);
    Priority_t priority = LOW_PRIORITY;
    if (urgent) {
        priority = HIGH_PRIORITY;
    } else if (slaType == SLA0) {
        priority = MID_PRIORITY;
    }

    VMId_t targetVM = VMId_t(-1);
    unsigned lowestTaskCount = UINT_MAX;

    // Iterate over all VMs and check for a candidate that meets the CPU and memory criteria.
    for (VMId_t vm : vms) {
        VMInfo_t info = VM_GetInfo(vm);
        if (info.cpu == requiredCPU) {
            // Check if the machine hosting this VM has enough free memory
            MachineInfo_t machineInfo = Machine_GetInfo(info.machine_id);
            if ((machineInfo.memory_size - machineInfo.memory_used) < taskInfo.required_memory) {
                continue; // Disqualify this VM due to insufficient memory
            }
            // Prefer empty VMs (especially for high-priority tasks)
            if (info.active_tasks.empty()) {
                targetVM = vm;
                break;
            }
            else if (info.active_tasks.size() < lowestTaskCount) {
                lowestTaskCount = info.active_tasks.size();
                targetVM = vm;
            }
        }
    }

    // If a suitable VM was found, assign the task
    if (targetVM != VMId_t(-1)) {
        VMInfo_t vmInfo = VM_GetInfo(targetVM);
        if (vmInfo.machine_id == MachineId_t(-1)) {
            return false;
        }
        VM_AddTask(targetVM, task_id, priority);
        // For high-priority tasks, ensure the machine is at maximum performance.
        return true;
    }
    return false;
}

void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
    // Attempt to assign the task immediately.
    // If no suitable VM is found, add the task to pendingTasks.
    if (!AssignTaskToVM(task_id, now)) {
        pendingTasks.insert(task_id);
    }
}

void Scheduler::PeriodicCheck(Time_t now) {
    // Instead of updating machine utilization, iterate over pendingTasks
    // and try to assign each task to a VM.
    for (auto it = pendingTasks.begin(); it != pendingTasks.end(); ) {
        if (AssignTaskToVM(*it, now)) {
            it = pendingTasks.erase(it);
        } else {
            ++it;
        }
    }
}

void Scheduler::TaskComplete(Time_t now, TaskId_t task_id) {
    // Update machine utilization after task completes
    for (MachineId_t machine : machines) {
        MachineInfo_t info = Machine_GetInfo(machine);
        double utilization = 0.0;
        if (info.num_cpus > 0) {
            utilization = static_cast<double>(info.active_tasks) / info.num_cpus;
        }
        machineUtilization[machine] = utilization;
    }
}

void Scheduler::Shutdown(Time_t time) {
    // Shut down all VMs
    for (auto &vm : vms) {
        VM_Shutdown(vm);
    }
    SimOutput("SimulationComplete(): Finished!", 4);
    SimOutput("SimulationComplete(): Time is " + std::to_string(time), 4);
}

void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id) {
    // Intentionally empty – placeholder for potential migration logic
}

// Scheduler entry points (global C-style functions)

void InitScheduler() {
    std::cout << "DIRECT OUTPUT: InitScheduler starting" << std::endl;
    std::cout.flush();
    scheduler.Init();
}

void HandleNewTask(Time_t time, TaskId_t task_id) {
    scheduler.NewTask(time, task_id);
}

void HandleTaskCompletion(Time_t time, TaskId_t task_id) {
    scheduler.TaskComplete(time, task_id);
}

void MemoryWarning(Time_t time, MachineId_t machine_id) {
    
}

void SchedulerCheck(Time_t time) {
    scheduler.PeriodicCheck(time);
}

void MigrationDone(Time_t time, VMId_t vm_id) {
    SimOutput("Migration done for vm " + std::to_string(vm_id), 4);
    scheduler.MigrationComplete(time, vm_id);
}

void SimulationComplete(Time_t time) {
    std::cout << "SLA violation report" << std::endl;
    std::cout << "SLA0: " << GetSLAReport(SLA0) << "%" << std::endl;
    std::cout << "SLA1: " << GetSLAReport(SLA1) << "%" << std::endl;
    std::cout << "SLA2: " << GetSLAReport(SLA2) << "%" << std::endl;
    std::cout << "Total Energy " << Machine_GetClusterEnergy() << "KW-Hour" << std::endl;
    std::cout << "Simulation run finished in " 
              << static_cast<double>(time) / 1000000 
              << " seconds" << std::endl;
    
    SimOutput("SimulationComplete(): Simulation finished at time " + std::to_string(time), 4);
    scheduler.Shutdown(time);
}

void StateChangeComplete(Time_t time, MachineId_t machine_id) {
    // Placeholder for handling state changes; can trigger a re-check if needed
    // scheduler.PeriodicCheck(time);
}

void SLAWarning(Time_t time, TaskId_t task_id) {
    SetTaskPriority(task_id, HIGH_PRIORITY);
}
