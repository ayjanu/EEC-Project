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
/**
 * A basic, stack-based first-fit scheduler:
 *  - Prefills VMs on initialization based on CPU type.
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
    
    // Create VMs for each active machine based on CPU type
    for (MachineId_t mach : activeMachines) {
        try {
            MachineInfo_t minfo = Machine_GetInfo(mach);
            if (minfo.memory_size < VM_MEMORY_OVERHEAD * 4) continue; // Skip if not enough memory
            
            CPUType_t cpuType = minfo.cpu;
            std::vector<VMId_t> machineVMs;
            
            // Create VMs based on CPU type capabilities
            if (cpuType == ARM || cpuType == X86) {
                // ARM and X86 can host WIN VMs
                VMId_t win1 = VM_Create(WIN, cpuType);
                VMId_t win2 = VM_Create(WIN, cpuType);
                VMId_t linux1 = VM_Create(LINUX, cpuType);
                VMId_t linux2 = VM_Create(LINUX_RT, cpuType);
                
                machineVMs = {win1, win2, linux1, linux2};
                SimOutput("Init: Created 2 WIN, 1 LINUX, 1 LINUX_RT VMs on machine " + std::to_string(mach), 3);
            } 
            else if (cpuType == POWER) {
                // POWER can host AIX VMs
                VMId_t aix1 = VM_Create(AIX, cpuType);
                VMId_t aix2 = VM_Create(AIX, cpuType);
                VMId_t linux1 = VM_Create(LINUX, cpuType);
                VMId_t linux2 = VM_Create(LINUX_RT, cpuType);
                
                machineVMs = {aix1, aix2, linux1, linux2};
                SimOutput("Init: Created 2 AIX, 1 LINUX, 1 LINUX_RT VMs on machine " + std::to_string(mach), 3);
            } 
            else {
                // For any other CPU types, create only LINUX and LINUX_RT VMs
                VMId_t linux1 = VM_Create(LINUX, cpuType);
                VMId_t linux2 = VM_Create(LINUX, cpuType);
                VMId_t linux3 = VM_Create(LINUX_RT, cpuType);
                VMId_t linux4 = VM_Create(LINUX_RT, cpuType);
                
                machineVMs = {linux1, linux2, linux3, linux4};
                SimOutput("Init: Created 2 LINUX, 2 LINUX_RT VMs on machine " + std::to_string(mach), 3);
            }
            
            // Attach VMs to the machine and add to tracking
            for (VMId_t vm : machineVMs) {
                VM_Attach(vm, mach);
                vmStack.push_back(vm);
                vms.push_back(vm);
                
                // Update VM type tracking
                VMInfo_t vmInfo = VM_GetInfo(vm);
                vmsByType[vmInfo.vm_type].insert(vm);
                vmsByMachine[mach].insert(vm);
            }
        } catch (...) {
            SimOutput("Init: Error creating VMs for machine " + std::to_string(mach), 2);
        }
    }
    
    SimOutput("Init: Scheduler initialized with " + std::to_string(vms.size()) + " VMs across " + 
              std::to_string(activeMachines.size()) + " active machines.", 2);
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
    
    // First check if we have VMs of the required type
    auto it = vmsByType.find(required_vm);
    if (it != vmsByType.end()) {
        for (VMId_t vm : it->second) {
            if (pendingMigrations.find(vm) != pendingMigrations.end()) continue;
            try {
                VMInfo_t vmInfo = VM_GetInfo(vm);
                if (vmInfo.cpu != required_cpu) continue;
                MachineInfo_t machInfo = Machine_GetInfo(vmInfo.machine_id);
                if (machInfo.s_state != S0) continue;
                if (machInfo.memory_used + required_mem > machInfo.memory_size) continue;
                target_vm = vm;
                break; // First fit: stop at the first suitable VM
            } catch (...) {}
        }
    }
    
    // If no suitable VM found by type, fall back to checking all VMs
    if (target_vm == VMId_t(-1)) {
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
                vmsByType[required_vm].insert(newVm);
                vmsByMachine[chosen_machine].insert(newVm);
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
    
    // Update VM tracking maps
    try {
        VMInfo_t vmInfo = VM_GetInfo(vm_id);
        MachineId_t newMachine = vmInfo.machine_id;
        
        // Find old machine
        MachineId_t oldMachine = MachineId_t(-1);
        for (auto& pair : vmsByMachine) {
            if (pair.second.find(vm_id) != pair.second.end()) {
                oldMachine = pair.first;
                break;
            }
        }
        
        // Update machine mapping
        if (oldMachine != MachineId_t(-1) && oldMachine != newMachine) {
            vmsByMachine[oldMachine].erase(vm_id);
            vmsByMachine[newMachine].insert(vm_id);
        }
    } catch (...) {
        SimOutput("MigrationComplete: Error updating VM tracking for VM " + std::to_string(vm_id), 2);
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
    
    // Clear tracking maps
    vmsByType.clear();
    vmsByMachine.clear();
    
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
void StateChangeComplete(Time_t time, MachineId_t machine_id) {
    // When a machine state change completes, check if it's now active
    try {
        MachineInfo_t info = Machine_GetInfo(machine_id);
        if (info.s_state == S0 && !scheduler.IsMachineActive(machine_id)) {
            // Machine is now active, add it to active set
            scheduler.ActivateMachine(machine_id);
            
            // Create VMs based on CPU type
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
                SimOutput("StateChange: Created 2 WIN, 1 LINUX, 1 LINUX_RT VMs on machine " + std::to_string(machine_id), 3);
            } 
            else if (cpuType == POWER) {
                // POWER can host AIX VMs
                VMId_t aix1 = VM_Create(AIX, cpuType);
                VMId_t aix2 = VM_Create(AIX, cpuType);
                VMId_t linux1 = VM_Create(LINUX, cpuType);
                VMId_t linux2 = VM_Create(LINUX_RT, cpuType);
                
                machineVMs = {aix1, aix2, linux1, linux2};
                SimOutput("StateChange: Created 2 AIX, 1 LINUX, 1 LINUX_RT VMs on machine " + std::to_string(machine_id), 3);
            } 
            else {
                // For any other CPU types, create only LINUX and LINUX_RT VMs
                VMId_t linux1 = VM_Create(LINUX, cpuType);
                VMId_t linux2 = VM_Create(LINUX, cpuType);
                VMId_t linux3 = VM_Create(LINUX_RT, cpuType);
                VMId_t linux4 = VM_Create(LINUX_RT, cpuType);
                
                machineVMs = {linux1, linux2, linux3, linux4};
                SimOutput("StateChange: Created 2 LINUX, 2 LINUX_RT VMs on machine " + std::to_string(machine_id), 3);
            }
            
            // Attach VMs to the machine and add to tracking
            for (VMId_t vm : machineVMs) {
                scheduler.AddVM(vm);
                VM_Attach(vm, machine_id);
                scheduler.vmStack.push_back(vm);
                
                // Update VM type tracking
                VMInfo_t vmInfo = VM_GetInfo(vm);
                scheduler.vmsByType[vmInfo.vm_type].insert(vm);
                scheduler.vmsByMachine[machine_id].insert(vm);
            }
        }
    } catch (...) {
        SimOutput("StateChangeComplete: Error handling state change for machine " + std::to_string(machine_id), 2);
    }
}
void SLAWarning(Time_t time, TaskId_t task_id) {
    // When an SLA warning is received, increase the task's priority
    try {
        SLAType_t slaType = RequiredSLA(task_id);
        if (slaType == SLA0 || slaType == SLA1) {
            SetTaskPriority(task_id, HIGH_PRIORITY);
            
            // Find which VM and machine this task is running on
            for (VMId_t vm : scheduler.GetVMs()) {
                VMInfo_t vmInfo = VM_GetInfo(vm);
                for (TaskId_t t : vmInfo.active_tasks) {
                    if (t == task_id) {
                        MachineId_t machine = vmInfo.machine_id;
                        
                        // Set the machine to maximum frequency
                        MachineInfo_t machineInfo = Machine_GetInfo(machine);
                        for (unsigned i = 0; i < machineInfo.num_cpus; i++) {
                            Machine_SetCorePerformance(machine, i, P0);
                        }
                        
                        SimOutput("SLAWarning: Set machine " + std::to_string(machine) + 
                                " to maximum frequency for task " + std::to_string(task_id), 2);
                        
                        break;
                    }
                }
            }
        }
    } catch (...) {
        SimOutput("SLAWarning: Error handling SLA warning for task " + std::to_string(task_id), 2);
    }
}