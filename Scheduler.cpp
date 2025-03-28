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

// CLAUDE AI used to debug and add logging functions

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
        
        // Initialize all machines with power-saving states
        for (unsigned core = 0; core < info.num_cpus; core++) {
            Machine_SetCorePerformance(machineId, core, P3);  // Start with lowest performance
            cpuStates[machineId][core] = P3;
        }
        // Set machine to active but power-saving state
        Machine_SetState(machineId, S0);
    }
    
    // Create VMs initially to handle the workload
    for (const auto &pair : machinesByCPU) {
        CPUType_t cpuType = pair.first;
        const std::vector<MachineId_t> &machinesWithCPU = pair.second;
        if (machinesWithCPU.empty()) continue;
        
        // Create VMs initially - up to 15 per CPU type
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
    TaskInfo_t tinfo = GetTaskInfo(task_id);
    bool urgent = false;
    if (tinfo.target_completion - (uint64_t)now <= 12000000) urgent = true;
    
    // Store task SLA for future reference
    taskSLAs[task_id] = sla_type;
    taskStartTimes[task_id] = now;
    
    // Prioritize tasks based on SLA and urgency
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
        
        // Adjust CPU performance based on task SLA
        MachineId_t machine = info.machine_id;
        MachineInfo_t machineInfo = Machine_GetInfo(machine);
        
        // Determine appropriate P-state based on SLA
        CPUPerformance_t pState;
        switch (sla_type) {
            case SLA0:
                pState = P0;  // Highest performance for strictest SLA
                break;
            case SLA1:
                pState = P0;  // Also high performance for SLA1
                break;
            case SLA2:
                pState = P1;  // Medium performance for SLA2
                break;
            case SLA3:
            default:
                pState = P2;  // Lower performance for best-effort tasks
                break;
        }
        
        // If task is urgent, use highest performance
        if (urgent) {
            pState = P0;
        }
        
        // Apply the P-state to the cores
        for (unsigned i = 0; i < machineInfo.num_cpus; i++) {
            // Only increase performance, never decrease
            if (pState < cpuStates[machine][i]) {
                Machine_SetCorePerformance(machine, i, pState);
                cpuStates[machine][i] = pState;
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
    
    // Collect information about tasks on each machine
    std::map<MachineId_t, bool> hasHighPriorityTasks;
    std::map<MachineId_t, bool> hasMediumPriorityTasks;
    std::map<MachineId_t, unsigned> taskCountPerMachine;
    std::map<MachineId_t, double> avgTaskProgress;
    
    for (MachineId_t machine : activeMachines) {
        hasHighPriorityTasks[machine] = false;
        hasMediumPriorityTasks[machine] = false;
        taskCountPerMachine[machine] = 0;
        avgTaskProgress[machine] = 0.0;
        
        // Check all VMs on this machine
        for (VMId_t vm : vms) {
            VMInfo_t vmInfo = VM_GetInfo(vm);
            if (vmInfo.machine_id != machine) continue;
            
            for (TaskId_t task : vmInfo.active_tasks) {
                taskCountPerMachine[machine]++;
                
                // Check SLA type
                SLAType_t slaType = RequiredSLA(task);
                if (slaType == SLA0) {
                    hasHighPriorityTasks[machine] = true;
                } else if (slaType == SLA1) {
                    hasMediumPriorityTasks[machine] = true;
                }
                
                // Check if task is at risk of SLA violation
                if (IsSLAViolation(task)) {
                    hasHighPriorityTasks[machine] = true;  // Treat as high priority
                }
                
                // Calculate task progress
                if (taskStartTimes.find(task) != taskStartTimes.end()) {
                    Time_t taskDuration = now - taskStartTimes[task];
                    TaskInfo_t taskInfo = GetTaskInfo(task);
                    Time_t expectedDuration = taskInfo.target_completion - taskStartTimes[task];
                    double progress = static_cast<double>(taskDuration) / expectedDuration;
                    avgTaskProgress[machine] += progress;
                }
            }
        }
        
        // Calculate average progress
        if (taskCountPerMachine[machine] > 0) {
            avgTaskProgress[machine] /= taskCountPerMachine[machine];
        }
    }
    
    // Adjust CPU performance based on collected data
    for (MachineId_t machine : activeMachines) {
        MachineInfo_t info = Machine_GetInfo(machine);
        unsigned taskCount = taskCountPerMachine[machine];
        
        if (taskCount == 0) {
            // No tasks - set to lowest performance or power down cores
            for (unsigned i = 0; i < info.num_cpus; i++) {
                Machine_SetCorePerformance(machine, i, P3);
                cpuStates[machine][i] = P3;
            }
            continue;
        }
        
        // Determine appropriate P-state based on machine conditions
        for (unsigned i = 0; i < info.num_cpus; i++) {
            CPUPerformance_t newPState;
            
            if (hasHighPriorityTasks[machine]) {
                // High priority tasks get maximum performance
                newPState = P0;
            } 
            else if (hasMediumPriorityTasks[machine]) {
                // Medium priority tasks
                if (machineUtilization[machine] > 0.7 || avgTaskProgress[machine] > 0.8) {
                    // High utilization or tasks nearing completion - boost performance
                    newPState = P0;
                } else {
                    newPState = P1;
                }
            }
            else {
                // Only low priority tasks
                if (machineUtilization[machine] > 0.8) {
                    // Very high utilization - boost performance
                    newPState = P0;
                } 
                else if (machineUtilization[machine] > 0.5) {
                    // Medium utilization
                    newPState = P1;
                }
                else if (machineUtilization[machine] > 0.3) {
                    // Lower utilization
                    newPState = P2;
                }
                else {
                    // Very low utilization
                    newPState = P3;
                }
            }
            
            // Apply the new P-state if it's different from current
            if (newPState != cpuStates[machine][i]) {
                Machine_SetCorePerformance(machine, i, newPState);
                cpuStates[machine][i] = newPState;
            }
        }
    }
}

void Scheduler::TaskComplete(Time_t now, TaskId_t task_id) {
    // Clean up task tracking
    taskSLAs.erase(task_id);
    taskStartTimes.erase(task_id);
    
    // Update machine utilization
    for (MachineId_t machine : machines) {
        MachineInfo_t info = Machine_GetInfo(machine);
        double utilization = 0.0;
        if (info.num_cpus > 0) {
            utilization = static_cast<double>(info.active_tasks) / info.num_cpus;
        }
        machineUtilization[machine] = utilization;
        
        // If machine has no tasks, reduce CPU performance
        if (info.active_tasks == 0) {
            for (unsigned i = 0; i < info.num_cpus; i++) {
                Machine_SetCorePerformance(machine, i, P3);
                cpuStates[machine][i] = P3;
            }
        }
    }
    
    // Run a check to potentially adjust other machines
    PeriodicCheck(now);
}

void Scheduler::Shutdown(Time_t time) {
    for(auto & vm: vms) {
        VM_Shutdown(vm);
    }
    SimOutput("SimulationComplete(): Finished!", 4);
    SimOutput("SimulationComplete(): Time is " + to_string(time), 4);
}

void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id) {
    // Adjust performance of source and destination machines
    PeriodicCheck(time);
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
        // Set to maximum performance to handle memory pressure
        MachineInfo_t machineInfo = Machine_GetInfo(machine_id);
        for (unsigned i = 0; i < machineInfo.num_cpus; i++) {
            Machine_SetCorePerformance(machine_id, i, P0);
            scheduler.cpuStates[machine_id][i] = P0;
        }
    } catch (...) {
        SimOutput("MemoryWarning 6 CAUGHT",1);
    }
}

void SchedulerCheck(Time_t time) {
    scheduler.PeriodicCheck(time);
}

void MigrationDone(Time_t time, VMId_t vm_id) {
    SimOutput("Migration done for vm " + to_string(vm_id), 4);
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
    // Run periodic check to update system state after machine state change
    scheduler.PeriodicCheck(time);
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
    if (taskVM != VMId_t(-1) && taskMachine != MachineId_t(-1)) {
        // Set task to highest priority
        SetTaskPriority(task_id, HIGH_PRIORITY);
        
        // Boost machine performance to maximum
        MachineInfo_t machineInfo = Machine_GetInfo(taskMachine);
        for (unsigned i = 0; i < machineInfo.num_cpus; i++) {
            Machine_SetCorePerformance(taskMachine, i, P0);
            scheduler.cpuStates[taskMachine][i] = P0;
        }
    }
}