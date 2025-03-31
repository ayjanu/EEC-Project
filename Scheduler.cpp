// CLAUDE used for debugging and adding logging
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
    std::map<CPUType_t, std::vector<MachineId_t>> machinesByCPU;
    std::vector<std::pair<unsigned, MachineId_t>> machineEfficiencies;
    for (unsigned i = 0; i < totalMachines; i++) {
        MachineId_t machineId = MachineId_t(i);
        machines.push_back(machineId);
        try {
            MachineInfo_t info = Machine_GetInfo(machineId);
            CPUType_t cpuType = info.cpu;
            machinesByCPU[cpuType].push_back(machineId);
            if (!info.s_states.empty() && info.s_states.size() > S0) {
                machineEfficiencies.push_back({info.s_states[S0], machineId});
            } else {
                machineEfficiencies.push_back({UINT_MAX, machineId});
            }
            if (info.s_state == S0) {
                activeMachines.insert(machineId);
                machineUtilization[machineId] = 0.0;
            } else {
                machineUtilization[machineId] = 0.0;
            }
        } catch (...) {
            SimOutput("Init: Error getting info for machine " + std::to_string(machineId), 1);
        }
    }
    std::sort(machineEfficiencies.begin(), machineEfficiencies.end());
    for (const auto& pair : machineEfficiencies) {
        sortedMachinesByEfficiency.push_back(pair.second);
    }
    unsigned initial_vms_per_type =  std::min(machines.size(), activeMachines.size() / (unsigned)machinesByCPU.size());;
    for (const auto &pair : machinesByCPU) {
        CPUType_t cpuType = pair.first;
        const std::vector<MachineId_t> &machinesWithCPU = pair.second;
        unsigned vms_created = 0;
        for (MachineId_t efficient_machine_id : sortedMachinesByEfficiency) {
            if (vms_created >= initial_vms_per_type) break;
            bool correct_cpu = false;
            for (MachineId_t m_id : machinesWithCPU) {
                if (m_id == efficient_machine_id) {
                    correct_cpu = true;
                    break;
                }
            }
            if (!correct_cpu) continue;
            if (activeMachines.count(efficient_machine_id)) {
                try {
                    MachineInfo_t mInfo = Machine_GetInfo(efficient_machine_id);
                    if (mInfo.memory_used + VM_MEMORY_OVERHEAD <= mInfo.memory_size) {
                        VMId_t vm = VM_Create(LINUX, cpuType);
                        vms.push_back(vm);
                        VM_Attach(vm, efficient_machine_id);
                        vms_created++;
                    }
                } catch (...) {
                    SimOutput("Init: Failed to create/attach initial VM on " + std::to_string(efficient_machine_id), 2);
                }
            }
        }
    }
    SimOutput("Init: Scheduler initialized with migration support.", 2);
}

bool Scheduler::SafeRemoveTask(VMId_t vm, TaskId_t task) {
    try {
        VM_RemoveTask(vm, task);
        return true;
    } catch (const std::runtime_error& e) {
        SimOutput("SafeRemoveTask failed for VM " + std::to_string(vm) + ", Task " + std::to_string(task) + ": " + e.what(), 2);
    } catch (...) {
        SimOutput("SafeRemoveTask failed for VM " + std::to_string(vm) + ", Task " + std::to_string(task) + ": Unknown error", 2);
    }
    return false;
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

MachineId_t Scheduler::FindMigrationTarget(VMId_t vm, Time_t now) {
    VMInfo_t vmInfo = VM_GetInfo(vm);
    CPUType_t required_cpu = vmInfo.cpu;
    unsigned total_mem_needed = VM_MEMORY_OVERHEAD;
    for (TaskId_t task : vmInfo.active_tasks) {
        total_mem_needed += GetTaskMemory(task);
    }
    for (MachineId_t machine : sortedMachinesByEfficiency) {
        if (machine == vmInfo.machine_id) continue;
        try {
            MachineInfo_t info = Machine_GetInfo(machine);
            if (info.cpu != required_cpu) continue;
            if (info.s_state != S0) {
                if (!activeMachines.count(machine)) {
                    Machine_SetState(machine, S0);
                    activeMachines.insert(machine);
                    machineUtilization[machine] = 0.0;
                }
                continue;
            }
            if (info.memory_used + total_mem_needed > info.memory_size) continue;
            double util = machineUtilization[machine];
            if (util < OVERLOAD_THRESHOLD) {
                SimOutput("FindMigrationTarget: Selected machine " + std::to_string(machine) + " for VM " + std::to_string(vm), 3);
                return machine;
            }
        } catch (...) {}
    }
    SimOutput("FindMigrationTarget: No suitable target found for VM " + std::to_string(vm), 2);
    return MachineId_t(-1);
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
            scheduler.machineUtilization[machine_id] = 0.0;
            needs_periodic_check = true;
        } else {
            SimOutput("StateChangeComplete: Machine " + std::to_string(machine_id) + " entered intermediate state " + std::to_string(currentState), 3);
            if (currentState >= S1 && currentState <= S4) {
                scheduler.machineUtilization[machine_id] = 0.0;
            }
        }
    } catch (...) {
        SimOutput("StateChangeComplete: Error getting info for machine " + std::to_string(machine_id) + ". Removing from active set as precaution.", 1);
        scheduler.DeactivateMachine(machine_id);
        scheduler.machineUtilization[machine_id] = 0.0;
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
            double util = scheduler.machineUtilization[taskMachine];
            if (util > scheduler.OVERLOAD_THRESHOLD) {
                MachineId_t target = scheduler.FindMigrationTarget(taskVM, time);
                if (target != MachineId_t(-1)) {
                    SimOutput("SLAWarning: Migrating VM " + std::to_string(taskVM) + " to " + std::to_string(target) + " due to overload", 2);
                    scheduler.pendingMigrations[taskVM] = target;
                    VM_Migrate(taskVM, target);
                }
            }
        } else if (slaType == SLA2 && GetTaskPriority(task_id) == LOW_PRIORITY) {
            SetTaskPriority(task_id, MID_PRIORITY);
        }
    }
}
