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
    
    // Create different VM assortments for each active machine based on CPU type
    for (MachineId_t machineId : activeMachines) {
        try {
            MachineInfo_t info = Machine_GetInfo(machineId);
            CPUType_t cpuType = info.cpu;
            
            // Check if there's enough memory for VMs
            if (info.memory_used + VM_MEMORY_OVERHEAD * 4 <= info.memory_size) {
                // Create VMs based on CPU type capabilities
                if (cpuType == ARM || cpuType == X86) {
                    // ARM and X86 can host WIN VMs
                    VMId_t win1 = VM_Create(WIN, cpuType);
                    VMId_t win2 = VM_Create(WIN, cpuType);
                    VMId_t linux1 = VM_Create(LINUX, cpuType);
                    VMId_t linux2 = VM_Create(LINUX_RT, cpuType);
                    
                    vms.push_back(win1);
                    vms.push_back(win2);
                    vms.push_back(linux1);
                    vms.push_back(linux2);
                    
                    VM_Attach(win1, machineId);
                    VM_Attach(win2, machineId);
                    VM_Attach(linux1, machineId);
                    VM_Attach(linux2, machineId);
                    
                    SimOutput("Init: Created 2 WIN, 1 LINUX, 1 LINUX_RT VMs on machine " + std::to_string(machineId), 3);
                } 
                else if (cpuType == POWER) {
                    // POWER can host AIX VMs
                    VMId_t aix1 = VM_Create(AIX, cpuType);
                    VMId_t aix2 = VM_Create(AIX, cpuType);
                    VMId_t linux1 = VM_Create(LINUX, cpuType);
                    VMId_t linux2 = VM_Create(LINUX_RT, cpuType);
                    
                    vms.push_back(aix1);
                    vms.push_back(aix2);
                    vms.push_back(linux1);
                    vms.push_back(linux2);
                    
                    VM_Attach(aix1, machineId);
                    VM_Attach(aix2, machineId);
                    VM_Attach(linux1, machineId);
                    VM_Attach(linux2, machineId);
                    
                    SimOutput("Init: Created 2 AIX, 1 LINUX, 1 LINUX_RT VMs on machine " + std::to_string(machineId), 3);
                } 
                else {
                    // For any other CPU types, create only LINUX and LINUX_RT VMs
                    VMId_t linux1 = VM_Create(LINUX, cpuType);
                    VMId_t linux2 = VM_Create(LINUX, cpuType);
                    VMId_t linux3 = VM_Create(LINUX_RT, cpuType);
                    VMId_t linux4 = VM_Create(LINUX_RT, cpuType);
                    
                    vms.push_back(linux1);
                    vms.push_back(linux2);
                    vms.push_back(linux3);
                    vms.push_back(linux4);
                    
                    VM_Attach(linux1, machineId);
                    VM_Attach(linux2, machineId);
                    VM_Attach(linux3, machineId);
                    VM_Attach(linux4, machineId);
                    
                    SimOutput("Init: Created 2 LINUX, 2 LINUX_RT VMs on machine " + std::to_string(machineId), 3);
                }
            }
        } catch (...) {
            SimOutput("Init: Failed to create VMs for machine " + std::to_string(machineId), 2);
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
    CPUType_t required_cpu = RequiredCPUType(task_id);
    VMType_t required_vm = RequiredVMType(task_id);
    SLAType_t sla_type = RequiredSLA(task_id);
    unsigned required_mem = GetTaskMemory(task_id);
    TaskInfo_t tindo = GetTaskInfo(task_id);
    bool urgent = false;
    if (tindo.target_completion - (uint64_t)now <= 12000000) urgent = true;
    Priority_t priority;
    switch (sla_type) {
        case SLA0: priority = HIGH_PRIORITY; break;
        case SLA1: priority = MID_PRIORITY; break;
        case SLA2: priority = LOW_PRIORITY; break;
        case SLA3:
        default:   priority = LOW_PRIORITY; break;
    }
    if (urgent) priority = HIGH_PRIORITY;
    VMId_t target_vm = VMId_t(-1);
    unsigned lowest_task_count = UINT_MAX;
    VMId_t least_loaded_compatible_vm = VMId_t(-1);
    VMId_t idle_compatible_vm = VMId_t(-1);
    for (VMId_t vm : vms) {
        if (pendingMigrations.find(vm) != pendingMigrations.end()) {
            SimOutput("NewTask " + std::to_string(task_id) + ": Skipping VM " + std::to_string(vm) + " due to pending migration.", 4);
            continue;
        }
        try {
            VMInfo_t vmInfo = VM_GetInfo(vm);
            if (vmInfo.machine_id == MachineId_t(-1)) continue;
            MachineInfo_t machInfo = Machine_GetInfo(vmInfo.machine_id);
            if (machInfo.s_state != S0) continue;
            if (vmInfo.cpu == required_cpu && vmInfo.vm_type == required_vm) {
                if (machInfo.memory_used + required_mem <= machInfo.memory_size) {
                    if (vmInfo.active_tasks.empty()) {
                        idle_compatible_vm = vm;
                        if (sla_type == SLA0 || sla_type == SLA1) {
                            target_vm = idle_compatible_vm;
                            break;
                        }
                    }
                    if (vmInfo.active_tasks.size() < lowest_task_count) {
                        lowest_task_count = vmInfo.active_tasks.size();
                        least_loaded_compatible_vm = vm;
                    }
                }
            }
        } catch (...) {}
    }
    if (target_vm == VMId_t(-1)) {
        if (idle_compatible_vm != VMId_t(-1)) {
            target_vm = idle_compatible_vm;
        } else if (least_loaded_compatible_vm != VMId_t(-1)) {
            target_vm = least_loaded_compatible_vm;
        }
    }
    if (target_vm == VMId_t(-1)) {
        MachineId_t target_machine = MachineId_t(-1);
        bool powered_on_new_machine = false;
        for (MachineId_t machine : sortedMachinesByEfficiency) {
            if (activeMachines.count(machine)) {
                try {
                    MachineInfo_t info = Machine_GetInfo(machine);
                    if (info.s_state != S0) continue;
                    if (info.cpu == required_cpu && (info.memory_used + required_mem + VM_MEMORY_OVERHEAD <= info.memory_size)) {
                        double current_utilization = machineUtilization[machine];
                        if ((sla_type == SLA0 || sla_type == SLA1) && current_utilization > 0.5) continue;
                        if (current_utilization > OVERLOAD_THRESHOLD) continue;
                        target_machine = machine;
                        break;
                    }
                } catch (...) {}
            }
        }
        if (target_machine == MachineId_t(-1)) {
            for (MachineId_t machine : sortedMachinesByEfficiency) {
                if (!activeMachines.count(machine)) {
                    try {
                        MachineInfo_t info = Machine_GetInfo(machine);
                        if (info.cpu == required_cpu && (required_mem + VM_MEMORY_OVERHEAD <= info.memory_size)) {
                            Machine_SetState(machine, S0);
                            target_machine = machine;
                            powered_on_new_machine = true;
                            break;
                        }
                    } catch (...) {}
                }
            }
        }
        if (target_machine != MachineId_t(-1)) {
            try {
                target_vm = VM_Create(required_vm, required_cpu);
                vms.push_back(target_vm);
                if (!powered_on_new_machine) {
                    VM_Attach(target_vm, target_machine);
                }
            } catch (...) {
                SimOutput("NewTask " + std::to_string(task_id) + ": VM Create/Attach failed on machine " + std::to_string(target_machine), 2);
                if (target_vm != VMId_t(-1)) {
                    vms.erase(std::remove(vms.begin(), vms.end(), target_vm), vms.end());
                }
                target_vm = VMId_t(-1);
            }
        }
    }
    if (target_vm != VMId_t(-1)) {
        try {
            VMInfo_t vmInfo = VM_GetInfo(target_vm);
            if (vmInfo.machine_id == MachineId_t(-1)) {
                SimOutput("NewTask " + std::to_string(task_id) + ": VM " + std::to_string(target_vm) + " not attached yet.", 2);
                return;
            }
            MachineInfo_t machInfo = Machine_GetInfo(vmInfo.machine_id);
            if (machInfo.s_state != S0) return;
            if (machInfo.memory_used + required_mem <= machInfo.memory_size) {
                VM_AddTask(target_vm, task_id, priority);
                if (sla_type == SLA0 || sla_type == SLA1) {
                    Machine_SetCorePerformance(vmInfo.machine_id, 0, P0);
                }
            } else {
                MemoryWarning(now, vmInfo.machine_id);
            }
        } catch (...) {
            SimOutput("NewTask " + std::to_string(task_id) + ": Failed to add task to VM " + std::to_string(target_vm), 2);
        }
    }
}
void Scheduler::PeriodicCheck(Time_t now) {
    for (MachineId_t machine : machines) {
        if (activeMachines.count(machine)) {
            try {
                MachineInfo_t info = Machine_GetInfo(machine);
                if (info.s_state == S0) {
                    double utilization = info.num_cpus > 0 ? static_cast<double>(info.active_tasks) / info.num_cpus : 0.0;
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
                if (util > 0.75) targetPState = P0;
                else if (util > 0.3) targetPState = P1;
                else targetPState = P2;
            }
            if (info.p_state != targetPState) {
                Machine_SetCorePerformance(machine, 0, targetPState);
            }
        } catch (...) {}
    }
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
                SimOutput("StateChangeComplete: No VM found on activated machine " + std::to_string(machine_id) + ". Creating VMs based on CPU type.", 3);
                try {
                    CPUType_t cpuType = machineInfo.cpu;
                    
                    // Create VMs based on CPU type capabilities
                    if (cpuType == ARM || cpuType == X86) {
                        // ARM and X86 can host WIN VMs
                        VMId_t win1 = VM_Create(WIN, cpuType);
                        VMId_t win2 = VM_Create(WIN, cpuType);
                        VMId_t linux1 = VM_Create(LINUX, cpuType);
                        VMId_t linux2 = VM_Create(LINUX_RT, cpuType);
                        
                        scheduler.AddVM(win1);
                        scheduler.AddVM(win2);
                        scheduler.AddVM(linux1);
                        scheduler.AddVM(linux2);
                        
                        VM_Attach(win1, machine_id);
                        VM_Attach(win2, machine_id);
                        VM_Attach(linux1, machine_id);
                        VM_Attach(linux2, machine_id);
                        
                        SimOutput("StateChangeComplete: Created 2 WIN, 1 LINUX, 1 LINUX_RT VMs on machine " + std::to_string(machine_id), 3);
                    } 
                    else if (cpuType == POWER) {
                        // POWER can host AIX VMs
                        VMId_t aix1 = VM_Create(AIX, cpuType);
                        VMId_t aix2 = VM_Create(AIX, cpuType);
                        VMId_t linux1 = VM_Create(LINUX, cpuType);
                        VMId_t linux2 = VM_Create(LINUX_RT, cpuType);
                        
                        scheduler.AddVM(aix1);
                        scheduler.AddVM(aix2);
                        scheduler.AddVM(linux1);
                        scheduler.AddVM(linux2);
                        
                        VM_Attach(aix1, machine_id);
                        VM_Attach(aix2, machine_id);
                        VM_Attach(linux1, machine_id);
                        VM_Attach(linux2, machine_id);
                        
                        SimOutput("StateChangeComplete: Created 2 AIX, 1 LINUX, 1 LINUX_RT VMs on machine " + std::to_string(machine_id), 3);
                    } 
                    else {
                        // For any other CPU types, create only LINUX and LINUX_RT VMs
                        VMId_t linux1 = VM_Create(LINUX, cpuType);
                        VMId_t linux2 = VM_Create(LINUX, cpuType);
                        VMId_t linux3 = VM_Create(LINUX_RT, cpuType);
                        VMId_t linux4 = VM_Create(LINUX_RT, cpuType);
                        
                        scheduler.AddVM(linux1);
                        scheduler.AddVM(linux2);
                        scheduler.AddVM(linux3);
                        scheduler.AddVM(linux4);
                        
                        VM_Attach(linux1, machine_id);
                        VM_Attach(linux2, machine_id);
                        VM_Attach(linux3, machine_id);
                        VM_Attach(linux4, machine_id);
                        
                        SimOutput("StateChangeComplete: Created 2 LINUX, 2 LINUX_RT VMs on machine " + std::to_string(machine_id), 3);
                    }
                } catch (const std::runtime_error& e) {
                    SimOutput("StateChangeComplete: Failed to create/attach VMs on " + std::to_string(machine_id) + ": " + e.what(), 2);
                } catch (...) {
                    SimOutput("StateChangeComplete: Unknown error creating/attaching VMs on " + std::to_string(machine_id), 2);
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