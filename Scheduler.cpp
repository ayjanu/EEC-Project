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
    unsigned totalMachines = Machine_GetTotal();
    std::map<CPUType_t, std::vector<MachineId_t>> machinesByCPU;
    for (unsigned i = 0; i < totalMachines; i++) {
        MachineId_t machineId = MachineId_t(i);
        machines.push_back(machineId);
        MachineInfo_t info = Machine_GetInfo(machineId);
        CPUType_t cpuType = info.cpu;
        machinesByCPU[cpuType].push_back(machineId);
    }
    
    // Create more VMs initially to handle the workload better
    for (const auto &pair : machinesByCPU) {
        CPUType_t cpuType = pair.first;
        const std::vector<MachineId_t> &machinesWithCPU = pair.second;
        if (machinesWithCPU.empty()) continue;
        
        // Create more VMs initially - up to 8 per CPU type
        unsigned numVMsToCreate = std::min(static_cast<unsigned>(machinesWithCPU.size()), 15u);
        for (unsigned i = 0; i < numVMsToCreate; i++) {
            VMId_t vm = VM_Create(LINUX, cpuType);
            vms.push_back(vm);
            MachineId_t machine = machinesWithCPU[i % machinesWithCPU.size()];
            VM_Attach(vm, machine);
            activeMachines.insert(machine);
            machineUtilization[machine] = 0.0;
        }
    }
}

void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
    CPUType_t required_cpu = RequiredCPUType(task_id);
    VMType_t required_vm = RequiredVMType(task_id);
    SLAType_t sla_type = RequiredSLA(task_id);
    TaskInfo_t tindo = GetTaskInfo(task_id);
    bool urgent = false;
    if (tindo.target_completion - (uint64_t)now <= 12000000) urgent = true;
    
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
        priority = LOW_PRIORITY;
        break;
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
        if (info.cpu == required_cpu) {
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
    // Update machine utilization
    for (MachineId_t machine : machines) {
        MachineInfo_t info = Machine_GetInfo(machine);
        double utilization = 0.0;
        if (info.num_cpus > 0) {
            utilization = static_cast<double>(info.active_tasks) / info.num_cpus;
        }
        machineUtilization[machine] = utilization;
    }
    
    // Adjust CPU performance based on load and SLA
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
            if (machineUtilization[machine] > 0.5) {
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
}

void Scheduler::TaskComplete(Time_t now, TaskId_t task_id) {
    // Update machine utilization
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
    for(auto & vm: vms) {
        VM_Shutdown(vm);
    }
    SimOutput("SimulationComplete(): Finished!", 4);
    SimOutput("SimulationComplete(): Time is " + to_string(time), 4);
}

void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id) {

}

void InitScheduler() {
    std::cout << "DIRECT OUTPUT: InitScheduler starting" << std::endl;
    std::cout.flush();
    scheduler.Init();
}

void HandleNewTask(Time_t time, TaskId_t task_id) {
    scheduler.NewTask(time, task_id);
}

void HandleTaskCompletion(Time_t time, TaskId_t task_id){
    scheduler.TaskComplete(time, task_id);
}

void MemoryWarning(Time_t time, MachineId_t machine_id) {
    try {
        MachineInfo_t machineInfo = Machine_GetInfo(machine_id);
        for (unsigned i = 0; i < machineInfo.num_cpus; i++) {
            Machine_SetCorePerformance(machine_id, i, P0);
        }
    } catch (...) {
        SimOutput("MemoryWarning 6 CAUGHT",1);
    }
}

void SchedulerCheck(Time_t time) {
    scheduler.PeriodicCheck(time);
}

void MigrationDone(Time_t time, VMId_t vm_id) {
    SimOutput("Migration done for vm " + vm_id,4);
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
    
    // Run periodic check to update system state
    // scheduler.PeriodicCheck(time);
}

void SLAWarning(Time_t time, TaskId_t task_id) {
    SLAType_t slaType = RequiredSLA(task_id);
    
    // Find which VM is running this task
    VMId_t taskVM = VMId_t(-1);
    MachineId_t taskMachine = MachineId_t(-1);
    for (VMId_t vm : scheduler.GetVMs()) {
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
        // For SLA0 and SLA1, take immediate action
        // if (slaType == SLA0 || slaType == SLA1) {
        //     // Set task to highest priority
        //     SetTaskPriority(task_id, HIGH_PRIORITY);
        // }
        // // For SLA2, just increase priority
        // else if (slaType == SLA2) {
        //     SetTaskPriority(task_id, MID_PRIORITY);
        // }
        SetTaskPriority(task_id, HIGH_PRIORITY);
    }
}