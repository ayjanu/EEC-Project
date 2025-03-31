//
//  Scheduler.cpp
//  CloudSim
//  Created by ELMOOTAZBELLAH ELNOZAHY on 10/20/24.
//
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

static Scheduler scheduler;
static int rr_index = 0;

bool HasHighPriorityTasks(MachineId_t machine_id) {
    for (VMId_t vm : scheduler.GetVMs()) {
        try {
            VMInfo_t vmInfo = VM_GetInfo(vm);
            if (vmInfo.machine_id != machine_id) continue;
            for (TaskId_t task : vmInfo.active_tasks) {
                try {
                    SLAType_t slaType = RequiredSLA(task);
                    if (slaType == SLA0 || slaType == SLA1) {
                        return true;
                    }
                } catch (...) {}
            }
        } catch (...) {}
    }
    return false;
}

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
    
    SimOutput("Round Robin Scheduler initialized with " + std::to_string(activeMachines.size()) + " machines and " + std::to_string(vms.size()) + " VMs", 0);
}




void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
    TaskInfo_t taskInfo = GetTaskInfo(task_id);
    SimOutput("Scheduler::NewTask(): Handling new task " + std::to_string(task_id), 3);

    unsigned totalMachines = Machine_GetTotal();
    for (unsigned i = 0; i < totalMachines; i++) {
        unsigned index = (rr_index + i) % totalMachines;
        MachineId_t machineId = MachineId_t(index);
        MachineInfo_t machineInfo = Machine_GetInfo(machineId);

        if (machineInfo.s_state != S0) continue;
        if (machineInfo.cpu != taskInfo.required_cpu) continue;
        if (machineInfo.memory_used + taskInfo.required_memory > machineInfo.memory_size) continue;

        // Inline FindOrCreateVM logic
        VMId_t vmId = VMId_t(-1);
        for (VMId_t vm : vms) {
            VMInfo_t vmInfo = VM_GetInfo(vm);
            if (vmInfo.machine_id == machineId && vmInfo.vm_type == taskInfo.required_vm) {
                vmId = vm;
                break;
            }
        }
        if (vmId == VMId_t(-1)) {
            vmId = VM_Create(taskInfo.required_vm, taskInfo.required_cpu);
            VM_Attach(vmId, machineId);
            vms.push_back(vmId);
        }

        if (vmId != VMId_t(-1)) {
            VM_AddTask(vmId, task_id, taskInfo.priority);
            SimOutput("Scheduler::NewTask(): Assigned task " + std::to_string(task_id) +
                      " to VM " + std::to_string(vmId), 2);
            rr_index = (index + 1) % totalMachines;
            return;
        }
    }

    for (unsigned i = 0; i < totalMachines; i++) {
        unsigned index = (rr_index + i) % totalMachines;
        MachineId_t machineId = MachineId_t(index);
        MachineInfo_t machineInfo = Machine_GetInfo(machineId);

        if (machineInfo.s_state != S5) continue;
        if (machineInfo.cpu != taskInfo.required_cpu) continue;

        Machine_SetState(machineId, S0);
        VMId_t newVmId = VM_Create(taskInfo.required_vm, taskInfo.required_cpu);
        VM_Attach(newVmId, machineId);
        VM_AddTask(newVmId, task_id, taskInfo.priority);

        vms.push_back(newVmId);
        machines.push_back(machineId);

        SimOutput("Scheduler::NewTask(): Activated machine " + std::to_string(machineId) +
                  " and assigned task " + std::to_string(task_id), 2);
        rr_index = (index + 1) % totalMachines;
        return;
    }

    SimOutput("Scheduler::NewTask(): Unable to assign task " + std::to_string(task_id), 1);
}


void Scheduler::PeriodicCheck(Time_t now) {
    
}

void Scheduler::TaskComplete(Time_t now, TaskId_t task_id) {
    SimOutput("TaskComplete: Task " + std::to_string(task_id) + " finished. Utilization update relies on PeriodicCheck.", 5);
}

void Scheduler::Shutdown(Time_t time) {
    SimOutput("Shutdown: Initiating simulation shutdown process.", 3);
    for(VMId_t vm : vms) {
        try {
            VMInfo_t vmInfo = VM_GetInfo(vm);
            if (vmInfo.machine_id != MachineId_t(-1)) {
                SimOutput("Shutdown: Shutting down VM " + std::to_string(vm) + " on machine " + std::to_string(vmInfo.machine_id), 4);
                VM_Shutdown(vm);
            }
        } catch (...) {
            SimOutput("Shutdown: Error getting info or shutting down VM " + std::to_string(vm), 2);
        }
    }
    SimOutput("SimulationComplete(): Finished!", 0);
    SimOutput("SimulationComplete(): Time is " + to_string(time), 0);
}

void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id) {
    auto it = pendingMigrations.find(vm_id);
    if (it != pendingMigrations.end()) {
        MachineId_t target = it->second;
        SimOutput("MigrationComplete: VM " + std::to_string(vm_id) + " migrated to machine " + std::to_string(target) + " at time " + std::to_string(time), 2);
        pendingMigrations.erase(it);
        try {
            VMInfo_t vmInfo = VM_GetInfo(vm_id);
            if (vmInfo.machine_id == target) {
                MachineInfo_t machInfo = Machine_GetInfo(target);
                if (HasHighPriorityTasks(target) && machInfo.p_state != P0) {
                    Machine_SetCorePerformance(target, 0, P0);
                }
            }
        } catch (...) {
            SimOutput("MigrationComplete: Error verifying migration for VM " + std::to_string(vm_id), 2);
        }
    } else {
        SimOutput("MigrationComplete: Unexpected migration completion for VM " + std::to_string(vm_id), 2);
    }
    PeriodicCheck(time);
}

void InitScheduler() {
    SimOutput("InitScheduler starting", 1);
    scheduler.Init();
    SimOutput("InitScheduler finished", 1);
}

void HandleNewTask(Time_t time, TaskId_t task_id) {
    scheduler.NewTask(time, task_id);
}

void HandleTaskCompletion(Time_t time, TaskId_t task_id){
    scheduler.TaskComplete(time, task_id);
}



void MemoryWarning(Time_t time, MachineId_t machine_id) {
    SimOutput("MemoryWarning: Memory pressure detected on machine " + std::to_string(machine_id) + " at time " + std::to_string(time), 1);
    try {
        MachineInfo_t machineInfo = Machine_GetInfo(machine_id);
        VMId_t largestVM = VMId_t(-1);
        unsigned mostTasks = 0;
        CPUType_t vmCpuType = POWER;
        for (VMId_t vm : scheduler.GetVMs()) {
            try {
                VMInfo_t vmInfo = VM_GetInfo(vm);
                if (vmInfo.machine_id == machine_id) {
                    if(vmInfo.active_tasks.size() >= mostTasks) {
                        mostTasks = vmInfo.active_tasks.size();
                        largestVM = vm;
                        vmCpuType = vmInfo.cpu;
                    }
                }
            } catch (...) {}
        }
        if (largestVM != VMId_t(-1)) {
            SimOutput("MemoryWarning: Largest VM " + std::to_string(largestVM) + " identified for potential action.", 2);
        } else {
            SimOutput("MemoryWarning: Could not identify a largest VM or it might be migrating.", 2);
        }
        SimOutput("MemoryWarning: Setting machine " + std::to_string(machine_id) + " to P0.", 3);
        try {
            for (unsigned i = 0; i < machineInfo.num_cpus; i++) {
                Machine_SetCorePerformance(machine_id, i, P0);
            }
        } catch (...) {
            SimOutput("MemoryWarning: Failed to set P0 for machine " + std::to_string(machine_id), 2);
        }
    } catch (...) {
        SimOutput("MemoryWarning: Error handling memory warning for machine " + std::to_string(machine_id), 1);
    }
}

void SchedulerCheck(Time_t time) {
    scheduler.PeriodicCheck(time);
}

void MigrationDone(Time_t time, VMId_t vm_id) {
    scheduler.MigrationComplete(time, vm_id);
}

void SimulationComplete(Time_t time) {
    std::cout << "SLA violation report" << std::endl;
    std::cout << "SLA0: " << GetSLAReport(SLA0) << "%" << std::endl;
    std::cout << "SLA1: " << GetSLAReport(SLA1) << "%" << std::endl;
    std::cout << "SLA2: " << GetSLAReport(SLA2) << "%" << std::endl;
    std::cout << "SLA3: " << GetSLAReport(SLA3) << "%" << std::endl;
    std::cout << "Total Energy " << Machine_GetClusterEnergy() << " KW-Hour" << std::endl;
    std::cout << "Simulation run finished in " << double(time)/1000000 << " seconds" << std::endl;
    SimOutput("SimulationComplete(): Final reporting done.", 0);
    scheduler.Shutdown(time);
}

void StateChangeComplete(Time_t time, MachineId_t machine_id) {
    SimOutput("StateChangeComplete: Machine " + std::to_string(machine_id) + " state change finished at time " + std::to_string(time), 3);
    bool needs_periodic_check = false;
    try {
        MachineInfo_t machineInfo = Machine_GetInfo(machine_id);
        MachineState_t currentState = machineInfo.s_state;
        if (currentState == S0) {
            SimOutput("StateChangeComplete: Machine " + std::to_string(machine_id) + " is now ACTIVE (S0). Adding to active set.", 2);
            scheduler.ActivateMachine(machine_id);
            SimOutput("StateChangeComplete: Setting initial P-state for machine " + std::to_string(machine_id) + " to P1.", 4);
            try {
                for (unsigned i = 0; i < machineInfo.num_cpus; i++) {
                    Machine_SetCorePerformance(machine_id, i, P1);
                }
            } catch(...) {
                SimOutput("StateChangeComplete: Failed to set initial P-state for " + std::to_string(machine_id), 2);
            }
            bool hasVM = false;
            for (VMId_t vm : scheduler.GetVMs()) {
                try {
                    VMInfo_t vmInfo = VM_GetInfo(vm);
                    if (vmInfo.machine_id == machine_id) {
                        hasVM = true;
                        break;
                    }
                } catch (...) {}
            }
            if (!hasVM) {
                SimOutput("StateChangeComplete: No VM found on activated machine " + std::to_string(machine_id) + ". Creating default VM.", 3);
                try {
                    VMId_t newVM = VM_Create(LINUX, machineInfo.cpu);
                    scheduler.AddVM(newVM);
                    VM_Attach(newVM, machine_id);
                    SimOutput("StateChangeComplete: Created and attached VM " + std::to_string(newVM) + " to machine " + std::to_string(machine_id), 3);
                } catch (const std::runtime_error& e) {
                    SimOutput("StateChangeComplete: Failed to create/attach default VM on " + std::to_string(machine_id) + ": " + e.what(), 2);
                } catch (...) {
                    SimOutput("StateChangeComplete: Unknown error creating/attaching default VM on " + std::to_string(machine_id), 2);
                }
            }
            needs_periodic_check = true;
        } else if (currentState == S5) {
            SimOutput("StateChangeComplete: Machine " + std::to_string(machine_id) + " is now OFF (S5). Removing from active set.", 2);
            scheduler.DeactivateMachine(machine_id);
            needs_periodic_check = true;
        } else {
            SimOutput("StateChangeComplete: Machine " + std::to_string(machine_id) + " entered intermediate state " + std::to_string(currentState), 3);
        }
    } catch (...) {
        SimOutput("StateChangeComplete: Error getting info for machine " + std::to_string(machine_id) + ". Removing from active set as precaution.", 1);
        scheduler.DeactivateMachine(machine_id);
        needs_periodic_check = true;
    }
    if (needs_periodic_check) {
        scheduler.PeriodicCheck(time);
    }
}

void SLAWarning(Time_t time, TaskId_t task_id) {
    SimOutput("SLAWarning: SLA violation predicted for task " + std::to_string(task_id), 1);
    SLAType_t slaType = RequiredSLA(task_id);
    VMId_t taskVM = VMId_t(-1);
    MachineId_t taskMachine = MachineId_t(-1);
    for (VMId_t vm : scheduler.GetVMs()) {
        if (scheduler.pendingMigrations.find(vm) != scheduler.pendingMigrations.end()) continue;
        try {
            VMInfo_t vmInfo = VM_GetInfo(vm);
            if (vmInfo.machine_id == MachineId_t(-1)) continue;
            for (TaskId_t task : vmInfo.active_tasks) {
                if (task == task_id) {
                    taskVM = vm;
                    taskMachine = vmInfo.machine_id;
                    break;
                }
            }
            if (taskVM != VMId_t(-1)) break;
        } catch (...) {}
    }
    if (taskVM != VMId_t(-1)) {
        if (slaType == SLA0 || slaType == SLA1) {
            SetTaskPriority(task_id, HIGH_PRIORITY);
            MachineInfo_t machInfo = Machine_GetInfo(taskMachine);
            if (machInfo.s_state == S0 && machInfo.p_state != P0) {
                Machine_SetCorePerformance(taskMachine, 0, P0);
            }
        } else if (slaType == SLA2 && GetTaskPriority(task_id) == LOW_PRIORITY) {
            SetTaskPriority(task_id, MID_PRIORITY);
        }
    }
}
