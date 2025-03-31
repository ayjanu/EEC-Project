//
//  Scheduler.cpp
//  CloudSim
//  Created by ELMOOTAZBELLAH ELNOZAHY on 10/20/24.
//
#include "Scheduler.hpp"
#include <string>
#include <iostream>
#include <climits>
#include <algorithm>

// The following code was written by Ayan Jannu and Leul Teka and cleaned/simplified by Claude 3.7.
// Thank you to the LLM for helping with the logic of using maps to track VM allocation and writing comments
// describing the processes being carried out as well as combining redudant code into helper functions.
/*
 * We use the shortest-first scheduling policy to get all task requests and sort them in order of target completion time.
 * The shortest runtimes get executed first, regardless of other parameters, in the hopes of mitigating SLA
 * violations as simply and fairly to all tasks as possible.
 */
// Global Scheduler instance
static Scheduler scheduler;

void Scheduler::Init() {
    unsigned totalMachines = Machine_GetTotal();
    
    // Gather all machines and organize by CPU type
    for (unsigned i = 0; i < totalMachines; i++) {
        MachineId_t machineId = MachineId_t(i);
        machines.push_back(machineId);
        MachineInfo_t info = Machine_GetInfo(machineId);
        CPUType_t cpuType = info.cpu;
        
        // Update the machinesByCPU map
        machinesByCPU[cpuType].insert(machineId);
        
        // Set all cores to maximum performance
        for (unsigned j = 0; j < info.num_cpus; j++) {
            Machine_SetCorePerformance(machineId, j, P0);
        }
        
        // Track utilization
        machineUtilization[machineId] = 0.0;
        activeMachines.insert(machineId);
    }
    
    // Create 4 VMs per machine with appropriate VM types based on CPU type
    for (MachineId_t machineId : machines) {
        MachineInfo_t info = Machine_GetInfo(machineId);
        CPUType_t cpuType = info.cpu;
        std::vector<VMId_t> machineVMs;
        
        // Create VMs based on CPU type capabilities
        if (cpuType == ARM || cpuType == X86) {
            // ARM and X86 can host WIN VMs
            VMId_t win1 = VM_Create(WIN, cpuType);
            VMId_t win2 = VM_Create(WIN, cpuType);
            VMId_t linux1 = VM_Create(LINUX, cpuType);
            VMId_t linux2 = VM_Create(LINUX_RT, cpuType);
            
            machineVMs = {win1, win2, linux1, linux2};
        } 
        else if (cpuType == POWER) {
            // POWER can host AIX VMs
            VMId_t aix1 = VM_Create(AIX, cpuType);
            VMId_t aix2 = VM_Create(AIX, cpuType);
            VMId_t linux1 = VM_Create(LINUX, cpuType);
            VMId_t linux2 = VM_Create(LINUX_RT, cpuType);
            
            machineVMs = {aix1, aix2, linux1, linux2};
        } 
        else {
            // For any other CPU types, create only LINUX and LINUX_RT VMs
            VMId_t linux1 = VM_Create(LINUX, cpuType);
            VMId_t linux2 = VM_Create(LINUX, cpuType);
            VMId_t linux3 = VM_Create(LINUX_RT, cpuType);
            VMId_t linux4 = VM_Create(LINUX_RT, cpuType);
            
            machineVMs = {linux1, linux2, linux3, linux4};
        }
        
        // Attach VMs to the machine and update tracking data structures
        for (VMId_t vm : machineVMs) {
            vms.push_back(vm);
            VM_Attach(vm, machineId);
            
            // Update the new tracking maps
            VMInfo_t vmInfo = VM_GetInfo(vm);
            vmsByType[vmInfo.vm_type].insert(vm);
            vmsByMachine[machineId].insert(vm);
        }
    }
    
    SimOutput("Shortest-First Scheduler initialized with " + std::to_string(activeMachines.size()) + " machines and " + std::to_string(vms.size()) + " VMs", 0);
}

// Helper function to try assigning a task to a VM
bool Scheduler::AssignTaskToVM(TaskId_t task_id, Time_t now) {
    CPUType_t requiredCPU = RequiredCPUType(task_id);
    SLAType_t slaType = RequiredSLA(task_id);
    TaskInfo_t taskInfo = GetTaskInfo(task_id);
    VMType_t requiredVMType = RequiredVMType(task_id);  // Assuming this function exists to get the VM type needed
    
    // Determine task urgency and base priority
    bool urgent = (taskInfo.target_completion - static_cast<uint64_t>(now) <= 12000000);
    Priority_t priority = LOW_PRIORITY;
    if (urgent) {
        priority = HIGH_PRIORITY;
    } else if (slaType == SLA0) {
        priority = MID_PRIORITY;
    }
    
    VMId_t targetVM = VMId_t(-1);
    VMId_t gpuVM = VMId_t(-1);
    unsigned lowestTaskCount = UINT_MAX;
    
    // First check VMs of the required type
    auto it = vmsByType.find(requiredVMType);
    if (it != vmsByType.end()) {
        for (VMId_t vm : it->second) {
            VMInfo_t info = VM_GetInfo(vm);
            
            // Check if VM has the required CPU type
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
                    if (machineInfo.gpus) gpuVM = vm;
                }
            }
        }
    }
    
    // If a suitable VM was found, assign the task
    if (targetVM != VMId_t(-1)) {
        VMInfo_t vmInfo = VM_GetInfo(targetVM);
        if (vmInfo.machine_id == MachineId_t(-1)) {
            return false;
        }
        MachineInfo_t mi = Machine_GetInfo(vmInfo.machine_id);
        if (!mi.gpus && gpuVM != (VMId_t)-1) targetVM = gpuVM;
        VM_AddTask(targetVM, task_id, priority);
        return true;
    }
    
    return false;
}

void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
    // Always add new tasks to pendingTasks first
    pendingTasksQueue.push(task_id);
}

void Scheduler::PeriodicCheck(Time_t now) {
    // First, move all tasks from the queue to the priority queue
    while (!pendingTasksQueue.empty()) {
        TaskId_t task_id = pendingTasksQueue.front();
        pendingTasksQueue.pop();
        
        // Add to the priority queue, which sorts by target_completion
        sortedPendingTasks.push(task_id);
    }
    
    // Try to assign pending tasks to VMs in order of target_completion
    while (!sortedPendingTasks.empty()) {
        TaskId_t task_id = sortedPendingTasks.top();
        
        if (AssignTaskToVM(task_id, now)) {
            // Task was successfully assigned, remove from queue
            sortedPendingTasks.pop();
        } else {
            // If we can't assign the highest priority task, we probably can't assign any
            // Keep it in the queue and try again later
            break;
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
    
    // Clear the tracking maps
    machinesByCPU.clear();
    vmsByType.clear();
    vmsByMachine.clear();
    
    // Clear the task queues
    while (!pendingTasksQueue.empty()) pendingTasksQueue.pop();
    while (!sortedPendingTasks.empty()) sortedPendingTasks.pop();
    
    SimOutput("SimulationComplete(): Finished!", 4);
    SimOutput("SimulationComplete(): Time is " + std::to_string(time), 4);
}

void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id) {
    // Update tracking maps after migration
    VMInfo_t vmInfo = VM_GetInfo(vm_id);
    
    // Find the old machine this VM was attached to
    for (auto& pair : vmsByMachine) {
        if (pair.second.find(vm_id) != pair.second.end()) {
            // Remove VM from old machine
            if (pair.first != vmInfo.machine_id) {
                pair.second.erase(vm_id);
                // Add VM to new machine
                vmsByMachine[vmInfo.machine_id].insert(vm_id);
            }
            break;
        }
    }
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
    // Could implement VM migration or task reallocation here
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
    scheduler.PeriodicCheck(time);
}

void SLAWarning(Time_t time, TaskId_t task_id) {
    SetTaskPriority(task_id, HIGH_PRIORITY);
}