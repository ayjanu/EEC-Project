//
//  Scheduler.cpp
//  CloudSim
//
#include "Scheduler.hpp"
#include <string>
#include <chrono>
#include <iomanip>
#include <sstream>
#include <iostream>
#include <climits>
#include "Internal_Interfaces.h"

// Global Scheduler instance
static Scheduler scheduler;

void Scheduler::Init() {
    SimOutput("E-Eco Scheduler starting", 1);
    
    unsigned totalMachines = Machine_GetTotal();
    std::map<CPUType_t, std::vector<MachineId_t>> machinesByCPU;
    
    // Initialize all machines
    for (unsigned i = 0; i < totalMachines; i++) {
        MachineId_t machineId = MachineId_t(i);
        machines.push_back(machineId);
        MachineInfo_t info = Machine_GetInfo(machineId);
        CPUType_t cpuType = info.cpu;
        machinesByCPU[cpuType].push_back(machineId);
    }
    
    // Calculate how many machines to make active, standby, and powered off
    unsigned activeCount = std::max(1u, totalMachines / 2);  // 50% active, at least 1
    unsigned standbyCount = std::max(1u, totalMachines / 4); // 25% standby, at least 1
    
    // Make sure we don't exceed total machines
    if (activeCount + standbyCount > totalMachines) {
        standbyCount = totalMachines - activeCount;
        if (standbyCount < 1) {
            standbyCount = 1;
            activeCount = totalMachines - standbyCount;
        }
    }
    
    // Set up active machines first
    for (unsigned i = 0; i < activeCount && i < totalMachines; i++) {
        MachineId_t machine = machines[i];
        activeMachines.insert(machine);
        machineStates[machine] = S0;
        Machine_SetState(machine, S0);
    }
    
    // Set up standby machines
    for (unsigned i = activeCount; i < (activeCount + standbyCount) && i < totalMachines; i++) {
        MachineId_t machine = machines[i];
        standbyMachines.insert(machine);
        machineStates[machine] = S2;
        Machine_SetState(machine, S2);
    }
    
    // Set up powered-off machines
    for (unsigned i = activeCount + standbyCount; i < totalMachines; i++) {
        MachineId_t machine = machines[i];
        poweredOffMachines.insert(machine);
        machineStates[machine] = S5;
        Machine_SetState(machine, S5);
    }
    
    // Create VMs on active machines
    for (MachineId_t machine : activeMachines) {
        MachineInfo_t info = Machine_GetInfo(machine);
        // Create VMs per active machine
        for (int j = 0; j < 5; j++) {  // Create 3 VMs per machine for better distribution
            VMId_t vm = VM_Create(LINUX, info.cpu);
            vms.push_back(vm);
            VM_Attach(vm, machine);
        }
    }
    
    SimOutput("E-Eco Scheduler initialized with " + 
              std::to_string(activeMachines.size()) + " active, " + 
              std::to_string(standbyMachines.size()) + " standby, and " + 
              std::to_string(poweredOffMachines.size()) + " powered-off machines", 1);
}

MachineId_t Scheduler::findLeastLoadedMachine(CPUType_t cpuType) const {
    MachineId_t bestMachine = MachineId_t(-1);
    unsigned lowestTaskCount = UINT_MAX;
    
    for (MachineId_t machine : activeMachines) {
        MachineInfo_t info = Machine_GetInfo(machine);
        if (info.cpu == cpuType) {
            if (info.active_tasks < lowestTaskCount) {
                lowestTaskCount = info.active_tasks;
                bestMachine = machine;
            }
        }
    }
    
    return bestMachine;
}

void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
    CPUType_t required_cpu = RequiredCPUType(task_id);
    SLAType_t sla_type = RequiredSLA(task_id);
    TaskInfo_t tinfo = GetTaskInfo(task_id);
    bool urgent = false;
    if (tinfo.target_completion - (uint64_t)now <= 12000000) urgent = true;
    
    // Prioritize SLA0 and SLA1 tasks with HIGH_PRIORITY
    Priority_t priority;
    switch (sla_type) {
    case SLA0:
        priority = HIGH_PRIORITY;
        break;
    case SLA1:
        priority = MID_PRIORITY;
        break;
    case SLA2:
    case SLA3:
    default:
        priority = LOW_PRIORITY;
        break;
    }
    if (urgent) priority = HIGH_PRIORITY;
    
    // Find the best VM for this task
    VMId_t target_vm = VMId_t(-1);
    unsigned lowest_task_count = UINT_MAX;
    
    for (VMId_t vm : vms) {
        VMInfo_t info = VM_GetInfo(vm);
        if (info.cpu == required_cpu && info.machine_id != MachineId_t(-1)) {
            // Prefer empty VMs for high-priority tasks
            if (info.active_tasks.empty()) {
                target_vm = vm;
                break;
            }
            else if (info.active_tasks.size() < lowest_task_count) {
                lowest_task_count = info.active_tasks.size();
                target_vm = vm;
            }
        }
    }
    
    // If no suitable VM found, try to activate a standby machine
    if (target_vm == VMId_t(-1) && !standbyMachines.empty()) {
        // Find a standby machine with the right CPU type
        for (MachineId_t machine : standbyMachines) {
            MachineInfo_t info = Machine_GetInfo(machine);
            if (info.cpu == required_cpu) {
                // Activate this machine
                standbyMachines.erase(machine);
                activeMachines.insert(machine);
                Machine_SetState(machine, S0);
                
                // Create a new VM on this machine
                VMId_t newVM = VM_Create(LINUX, required_cpu);
                vms.push_back(newVM);
                VM_Attach(newVM, machine);
                target_vm = newVM;
                break;
            }
        }
    }
    
    // If still no VM, try to power on a machine
    if (target_vm == VMId_t(-1) && !poweredOffMachines.empty()) {
        // Find a powered-off machine with the right CPU type
        for (MachineId_t machine : poweredOffMachines) {
            MachineInfo_t info = Machine_GetInfo(machine);
            if (info.cpu == required_cpu) {
                // Power on this machine
                poweredOffMachines.erase(machine);
                activeMachines.insert(machine);
                Machine_SetState(machine, S0);
                
                // Create a new VM on this machine
                VMId_t newVM = VM_Create(LINUX, required_cpu);
                vms.push_back(newVM);
                VM_Attach(newVM, machine);
                target_vm = newVM;
                break;
            }
        }
    }
    
    // Assign task to VM
    if (target_vm != VMId_t(-1)) {
        VMInfo_t info = VM_GetInfo(target_vm);
        if (info.machine_id == MachineId_t(-1)) return;
        
        VM_AddTask(target_vm, task_id, priority);
        
        // For SLA0/SLA1, ensure the machine is at maximum performance
        if (sla_type == SLA0 || sla_type == SLA1) {
            MachineId_t machine = info.machine_id;
            MachineInfo_t machineInfo = Machine_GetInfo(machine);
            for (unsigned i = 0; i < machineInfo.num_cpus; i++) {
                Machine_SetCorePerformance(machine, i, P0);
            }
        }
    }
}

void Scheduler::PeriodicCheck(Time_t now) {
    // Update task counts for each machine
    std::map<MachineId_t, unsigned> machineTasks;
    for (MachineId_t machine : machines) {
        MachineInfo_t info = Machine_GetInfo(machine);
        machineTasks[machine] = info.active_tasks;
    }
    
    // Adjust CPU performance based on load and SLA for active machines
    for (MachineId_t machine : activeMachines) {
        MachineInfo_t info = Machine_GetInfo(machine);
        
        // Check for SLA0/SLA1 tasks on this machine
        bool hasHighPriorityTasks = false;
        
        for (VMId_t vm : vms) {
            VMInfo_t vmInfo = VM_GetInfo(vm);
            if (vmInfo.machine_id != machine) continue;
            
            for (TaskId_t task : vmInfo.active_tasks) {
                SLAType_t slaType = RequiredSLA(task);
                if (slaType == SLA0 || slaType == SLA1) {
                    hasHighPriorityTasks = true;
                    break;
                }
            }
            if (hasHighPriorityTasks) break;
        }
        
        // Set CPU performance based on tasks
        if (hasHighPriorityTasks) {
            // Maximum performance for machines with high-priority tasks
            for (unsigned i = 0; i < info.num_cpus; i++) {
                Machine_SetCorePerformance(machine, i, P0);
            }
        }
        else if (info.active_tasks > 0) {
            // For machines with only lower-priority tasks, use P0 if utilization is high
            double utilization = static_cast<double>(info.active_tasks) / info.num_cpus;
            if (utilization > 0.5) {
                for (unsigned i = 0; i < info.num_cpus; i++) {
                    Machine_SetCorePerformance(machine, i, P0);
                }
            } else {
                // Use P1 for moderate utilization
                for (unsigned i = 0; i < info.num_cpus; i++) {
                    Machine_SetCorePerformance(machine, i, P1);
                }
            }
        }
        else {
            // No active tasks, use lowest performance
            for (unsigned i = 0; i < info.num_cpus; i++) {
                Machine_SetCorePerformance(machine, i, P3);
            }
        }
    }
    
    // Don't try to put machines into standby in this simplified version
    // This was causing the segmentation fault
}

void Scheduler::TaskComplete(Time_t now, TaskId_t task_id) {
    // Update task counts
    std::map<MachineId_t, unsigned> machineTasks;
    for (MachineId_t machine : machines) {
        MachineInfo_t info = Machine_GetInfo(machine);
        machineTasks[machine] = info.active_tasks;
    }
    
    // No standby transitions in this simplified version
}

void Scheduler::HandleMemoryWarning(Time_t time, MachineId_t machine_id) {
    // Activate a standby machine if available
    if (!standbyMachines.empty()) {
        MachineId_t machine = *standbyMachines.begin();
        standbyMachines.erase(machine);
        activeMachines.insert(machine);
        Machine_SetState(machine, S0);
        
        // Create a VM on the newly activated machine
        MachineInfo_t info = Machine_GetInfo(machine);
        VMId_t newVM = VM_Create(LINUX, info.cpu);
        vms.push_back(newVM);
        VM_Attach(newVM, machine);
    }
    
    // Set machine to maximum performance
    MachineInfo_t machineInfo = Machine_GetInfo(machine_id);
    for (unsigned i = 0; i < machineInfo.num_cpus; i++) {
        Machine_SetCorePerformance(machine_id, i, P0);
    }
}

void Scheduler::HandleSLAWarning(Time_t time, TaskId_t task_id) {
    // Find which VM is running this task
    VMId_t taskVM = VMId_t(-1);
    MachineId_t taskMachine = MachineId_t(-1);
    
    for (VMId_t vm : vms) {
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
    }
    
    // Take action based on SLA type
    if (taskVM != VMId_t(-1)) {
        // Set task to highest priority
        SetTaskPriority(task_id, HIGH_PRIORITY);
        
        // Set machine to maximum performance
        MachineInfo_t machineInfo = Machine_GetInfo(taskMachine);
        for (unsigned i = 0; i < machineInfo.num_cpus; i++) {
            Machine_SetCorePerformance(taskMachine, i, P0);
        }
        
        // Activate more machines if needed
        if (!standbyMachines.empty()) {
            MachineId_t machine = *standbyMachines.begin();
            standbyMachines.erase(machine);
            activeMachines.insert(machine);
            Machine_SetState(machine, S0);
            
            // Create a VM on the newly activated machine
            MachineInfo_t info = Machine_GetInfo(machine);
            VMId_t newVM = VM_Create(LINUX, info.cpu);
            vms.push_back(newVM);
            VM_Attach(newVM, machine);
        }
    }
}

void Scheduler::HandleStateChangeComplete(Time_t time, MachineId_t machine_id) {
    MachineInfo_t info = Machine_GetInfo(machine_id);
    MachineState_t newState = info.s_state;
    
    SimOutput("Machine " + std::to_string(machine_id) + " completed state change to " + 
              std::to_string(newState), 2);
    
    // Update our machine state tracking
    machineStates[machine_id] = newState;
    
    // If this is a newly activated machine and doesn't have a VM, create one
    if (newState == S0) {
        bool hasVM = false;
        for (VMId_t vm : vms) {
            VMInfo_t vmInfo = VM_GetInfo(vm);
            if (vmInfo.machine_id == machine_id) {
                hasVM = true;
                break;
            }
        }
        
        if (!hasVM) {
            MachineInfo_t machineInfo = Machine_GetInfo(machine_id);
            VMId_t newVM = VM_Create(LINUX, machineInfo.cpu);
            vms.push_back(newVM);
            VM_Attach(newVM, machine_id);
        }
    }
}

void Scheduler::Shutdown(Time_t time) {
    for(auto & vm: vms) {
        VM_Shutdown(vm);
    }
    SimOutput("SimulationComplete(): Finished!", 4);
    SimOutput("SimulationComplete(): Time is " + to_string(time), 4);
}

void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id) {
    // Nothing special to do here in this simplified version
}

// Interface functions
void InitScheduler() {
    std::cout << "DIRECT OUTPUT: E-Eco Scheduler starting" << std::endl;
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
    try {
        scheduler.HandleMemoryWarning(time, machine_id);
    } catch (...) {
        SimOutput("MemoryWarning exception caught", 1);
    }
}

void SchedulerCheck(Time_t time) {
    scheduler.PeriodicCheck(time);
}

void MigrationDone(Time_t time, VMId_t vm_id) {
    SimOutput("Migration done for vm " + std::to_string(vm_id), 4);
    scheduler.MigrationComplete(time, vm_id);
}

void SimulationComplete(Time_t time) {
    cout << "SLA violation report" << endl;
    cout << "SLA0: " << GetSLAReport(SLA0) << "%" << endl;
    cout << "SLA1: " << GetSLAReport(SLA1) << "%" << endl;
    cout << "SLA2: " << GetSLAReport(SLA2) << "%" << endl;
    cout << "Total Energy " << Machine_GetClusterEnergy() << "KW-Hour" << endl;
    cout << "Simulation run finished in " << double(time)/1000000 << " seconds" << endl;
    SimOutput("SimulationComplete(): Simulation finished at time " + to_string(time), 4);
    scheduler.Shutdown(time);
}

void StateChangeComplete(Time_t time, MachineId_t machine_id) {
    scheduler.HandleStateChangeComplete(time, machine_id);
}

void SLAWarning(Time_t time, TaskId_t task_id) {
    scheduler.HandleSLAWarning(time, task_id);
}