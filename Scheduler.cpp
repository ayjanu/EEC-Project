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

// The following code was written by Ayan Jannu and Leul Teka and cleaned/simplified by Claude 3.7.
// Thank you to the LLM for helping with the logic of using maps to track VM allocation and writing comments
// describing the processes being carried out as well as combining redudant code into helper functions.

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
      
       // Set initial machine state based on index
       if (i < totalMachines / 3) {
           // First third of machines are active
           activeMachines.insert(machineId);
           machineUtilization[machineId] = 0.0;
          
           // Set all cores to maximum performance initially
           for (unsigned j = 0; j < info.num_cpus; j++) {
               Machine_SetCorePerformance(machineId, j, P0);
           }
       }
       else if (i < totalMachines * 2 / 3) {
           // Second third of machines are in standby
           Machine_SetState(machineId, S0i1);
       }
       else {
           // Last third of machines are powered off
           Machine_SetState(machineId, S5);
       }
   }
  
   // Create VMs only on active machines
   for (MachineId_t machineId : activeMachines) {
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
          
           // Update the tracking maps
           VMInfo_t vmInfo = VM_GetInfo(vm);
           vmsByType[vmInfo.vm_type].insert(vm);
           vmsByMachine[machineId].insert(vm);
       }
   }
  
   // Initial DVFS adjustment
   adjustCPUFrequencies();
  
   SimOutput("e-eco Scheduler initialized with " + std::to_string(activeMachines.size()) +
             " active machines and " + std::to_string(vms.size()) + " VMs", 0);
}


// Method to wake up a machine
void Scheduler::WakeUpMachine(MachineId_t machine) {
   // Get the current state of the machine
   MachineInfo_t info = Machine_GetInfo(machine);
  
   // Only proceed if the machine is in standby or powered off
   if (info.s_state == S0i1 || info.s_state == S5) {
       // Mark the machine as waking up
       wakingUpMachines.insert(machine);
      
       // Request state change to S0
       Machine_SetState(machine, S0);
       SimOutput("Waking up machine " + std::to_string(machine), 2);
   }
}


// Method called when a machine has completed waking up
void Scheduler::MachineWakeUpComplete(MachineId_t machine) {
   // Only proceed if the machine was marked as waking up
   if (wakingUpMachines.find(machine) != wakingUpMachines.end()) {
       // Remove from waking up set
       wakingUpMachines.erase(machine);
      
       // Add to active machines
       activeMachines.insert(machine);
       machineUtilization[machine] = 0.0;
      
       // Get machine info
       MachineInfo_t info = Machine_GetInfo(machine);
       CPUType_t cpuType = info.cpu;
      
       // Set initial CPU frequency based on expected load
       for (unsigned i = 0; i < info.num_cpus; i++) {
           Machine_SetCorePerformance(machine, i, P0);  // Start with max performance
       }
      
       // Create VMs for this machine
       std::vector<VMId_t> machineVMs;
      
       // Create VMs based on CPU type capabilities
       if (cpuType == ARM || cpuType == X86) {
           VMId_t win1 = VM_Create(WIN, cpuType);
           VMId_t win2 = VM_Create(WIN, cpuType);
           VMId_t linux1 = VM_Create(LINUX, cpuType);
           VMId_t linux2 = VM_Create(LINUX_RT, cpuType);
           machineVMs = {win1, win2, linux1, linux2};
       }
       else if (cpuType == POWER) {
           VMId_t aix1 = VM_Create(AIX, cpuType);
           VMId_t aix2 = VM_Create(AIX, cpuType);
           VMId_t linux1 = VM_Create(LINUX, cpuType);
           VMId_t linux2 = VM_Create(LINUX_RT, cpuType);
           machineVMs = {aix1, aix2, linux1, linux2};
       }
       else {
           VMId_t linux1 = VM_Create(LINUX, cpuType);
           VMId_t linux2 = VM_Create(LINUX, cpuType);
           VMId_t linux3 = VM_Create(LINUX_RT, cpuType);
           VMId_t linux4 = VM_Create(LINUX_RT, cpuType);
           machineVMs = {linux1, linux2, linux3, linux4};
       }
      
       // Attach VMs to the machine and update tracking
       for (VMId_t vm : machineVMs) {
           vms.push_back(vm);
           VM_Attach(vm, machine);
          
           VMInfo_t vmInfo = VM_GetInfo(vm);
           vmsByType[vmInfo.vm_type].insert(vm);
           vmsByMachine[machine].insert(vm);
       }
      
       SimOutput("Machine " + std::to_string(machine) + " is now fully awake", 2);
   }
}


// Determine the appropriate CPU frequency based on utilization
CPUPerformance_t Scheduler::determineCPUFrequency(double utilization) {
   if (utilization >= P0_THRESHOLD) {
       return P0;  // High utilization -> maximum frequency
   } else if (utilization >= P1_THRESHOLD) {
       return P1;  // Medium-high utilization -> 3/4 frequency
   } else if (utilization >= P2_THRESHOLD) {
       return P2;  // Medium-low utilization -> 1/2 frequency
   } else {
       return P3;  // Low utilization -> 1/4 frequency
   }
}


// Adjust CPU frequencies for all active machines based on their utilization
void Scheduler::adjustCPUFrequencies() {
   for (MachineId_t machine : activeMachines) {
       // Skip machines that are waking up
       if (IsMachineWakingUp(machine)) {
           continue;
       }
      
       double utilization = machineUtilization[machine];
       CPUPerformance_t targetFrequency = determineCPUFrequency(utilization);
      
       // Get machine info
       MachineInfo_t info = Machine_GetInfo(machine);
      
       // Check if the machine has any high-priority tasks
       bool hasHighPriorityTasks = false;
       if (vmsByMachine.find(machine) != vmsByMachine.end()) {
           for (VMId_t vm : vmsByMachine[machine]) {
               VMInfo_t vmInfo = VM_GetInfo(vm);
               for (TaskId_t task : vmInfo.active_tasks) {
                   if (GetTaskPriority(task) == HIGH_PRIORITY) {
                       hasHighPriorityTasks = true;
                       break;
                   }
               }
               if (hasHighPriorityTasks) break;
           }
       }
      
       // If the machine has high-priority tasks, ensure it runs at maximum frequency
       if (hasHighPriorityTasks) {
           targetFrequency = P0;
       }
      
       // Set all cores to the target frequency
       for (unsigned i = 0; i < info.num_cpus; i++) {
           Machine_SetCorePerformance(machine, i, targetFrequency);
       }
      
       // Log frequency changes
       std::string frequencyStr;
       switch (targetFrequency) {
           case P0: frequencyStr = "maximum (P0)"; break;
           case P1: frequencyStr = "3/4 (P1)"; break;
           case P2: frequencyStr = "1/2 (P2)"; break;
           case P3: frequencyStr = "1/4 (P3)"; break;
       }
      
       SimOutput("Set machine " + std::to_string(machine) + " CPU frequency to " +
                frequencyStr + " (utilization: " + std::to_string(utilization) + ")", 3);
   }
}


// Helper function to try assigning a task to a VM
bool Scheduler::AssignTaskToVM(TaskId_t task_id, Time_t now) {
   CPUType_t requiredCPU = RequiredCPUType(task_id);
   SLAType_t slaType = RequiredSLA(task_id);
   TaskInfo_t taskInfo = GetTaskInfo(task_id);
   VMType_t requiredVMType = RequiredVMType(task_id);
  
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
               // Check if the machine hosting this VM is active
               if (activeMachines.find(info.machine_id) == activeMachines.end()) {
                   continue;
               }
              
               // Skip machines that are waking up
               if (IsMachineWakingUp(info.machine_id)) {
                   continue;
               }
              
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
      
       // If this is a high-priority task, ensure the machine is at maximum frequency
       if (priority == HIGH_PRIORITY) {
           MachineId_t machine = vmInfo.machine_id;
           MachineInfo_t machineInfo = Machine_GetInfo(machine);
          
           for (unsigned i = 0; i < machineInfo.num_cpus; i++) {
               Machine_SetCorePerformance(machine, i, P0);
           }
          
           SimOutput("Set machine " + std::to_string(machine) +
                    " to maximum frequency for high-priority task", 3);
       }
      
       return true;
   }
  
   // If no suitable VM exists, check if we need to wake up a machine
   if (activeMachines.size() < machines.size() / 2) {  // Only wake up if we're using less than half the machines
       // First try to find a machine that's already waking up with the right CPU type
       for (MachineId_t machine : wakingUpMachines) {
           MachineInfo_t info = Machine_GetInfo(machine);
           if (info.cpu == requiredCPU) {
               // A suitable machine is already waking up, wait for it
               return false;
           }
       }
      
       // Next, look for standby machines with the right CPU type
       for (MachineId_t machine : machines) {
           MachineInfo_t info = Machine_GetInfo(machine);
          
           // Skip active and waking up machines
           if (activeMachines.find(machine) != activeMachines.end() ||
               wakingUpMachines.find(machine) != wakingUpMachines.end()) {
               continue;
           }
          
           // Check if this is a standby machine with the right CPU type
           if (info.s_state == S0i1 && info.cpu == requiredCPU) {
               // Wake up this machine
               WakeUpMachine(machine);
               return false;  // Task remains pending until machine wakes up
           }
       }
      
       // If no suitable standby machine, try powered off machines
       for (MachineId_t machine : machines) {
           MachineInfo_t info = Machine_GetInfo(machine);
          
           // Skip active, waking up, and standby machines
           if (activeMachines.find(machine) != activeMachines.end() ||
               wakingUpMachines.find(machine) != wakingUpMachines.end() ||
               info.s_state == S0i1) {
               continue;
           }
          
           // Check if this is a powered off machine with the right CPU type
           if (info.s_state == S5 && info.cpu == requiredCPU) {
               // Power on this machine
               WakeUpMachine(machine);
               return false;  // Task remains pending until machine wakes up
           }
       }
   }
  
   // We couldn't assign the task right now
   return false;
}


void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
   // Attempt to assign the task immediately.
   // If no suitable VM is found, add the task to pendingTasks.
   if (!AssignTaskToVM(task_id, now)) {
       pendingTasks.insert(task_id);
   }
  
   // After adding a task, adjust CPU frequencies
   adjustCPUFrequencies();
}


void Scheduler::PeriodicCheck(Time_t now) {
   // Try to assign pending tasks
   for (auto it = pendingTasks.begin(); it != pendingTasks.end(); ) {
       if (AssignTaskToVM(*it, now)) {
           it = pendingTasks.erase(it);
       } else {
           ++it;
       }
   }
  
   // Update machine utilization
   for (MachineId_t machine : activeMachines) {
       // Skip machines that are waking up
       if (IsMachineWakingUp(machine)) {
           continue;
       }
      
       MachineInfo_t info = Machine_GetInfo(machine);
       double utilization = 0.0;
       if (info.num_cpus > 0) {
           utilization = static_cast<double>(info.active_tasks) / info.num_cpus;
       }
       machineUtilization[machine] = utilization;
   }
  
   // Adjust CPU frequencies based on current utilization
   adjustCPUFrequencies();
  
   // Check if we can put any machines in standby
   if (now % 10000000 == 0) {  // Every 10 seconds of simulation time
       // Find machines with low utilization
       std::vector<MachineId_t> lowUtilizationMachines;
       for (MachineId_t machine : activeMachines) {
           // Skip machines that are waking up
           if (IsMachineWakingUp(machine)) {
               continue;
           }
          
           if (machineUtilization[machine] < UNDERLOAD_THRESHOLD) {
               // Check if this machine has any active tasks
               MachineInfo_t info = Machine_GetInfo(machine);
               if (info.active_tasks == 0) {
                   lowUtilizationMachines.push_back(machine);
               }
           }
       }
      
       // Keep at least 2 machines active at all times
       if (activeMachines.size() - lowUtilizationMachines.size() >= 2) {
           // Put up to 2 machines in standby mode
           unsigned machinesStandby = 0;
           for (MachineId_t machine : lowUtilizationMachines) {
               if (machinesStandby >= 2) break;
              
               // Shutdown all VMs on this machine
               if (vmsByMachine.find(machine) != vmsByMachine.end()) {
                   std::vector<VMId_t> vmsToShutdown(vmsByMachine[machine].begin(), vmsByMachine[machine].end());
                  
                   for (VMId_t vm : vmsToShutdown) {
                       VM_Shutdown(vm);
                       vms.erase(std::remove(vms.begin(), vms.end(), vm), vms.end());
                      
                       // Remove from type mapping
                       VMInfo_t vmInfo = VM_GetInfo(vm);
                       if (vmsByType.find(vmInfo.vm_type) != vmsByType.end()) {
                           vmsByType[vmInfo.vm_type].erase(vm);
                       }
                   }
                  
                   // Clear the machine's VM list
                   vmsByMachine[machine].clear();
               }
              
               // Put machine in standby
               activeMachines.erase(machine);
               Machine_SetState(machine, S0i1);
               machinesStandby++;
              
               SimOutput("Put machine " + std::to_string(machine) + " in standby mode", 2);
           }
       }
   }
}


void Scheduler::TaskComplete(Time_t now, TaskId_t task_id) {
   // Update machine utilization after task completes
   for (MachineId_t machine : activeMachines) {
       // Skip machines that are waking up
       if (IsMachineWakingUp(machine)) {
           continue;
       }
      
       MachineInfo_t info = Machine_GetInfo(machine);
       double utilization = 0.0;
       if (info.num_cpus > 0) {
           utilization = static_cast<double>(info.active_tasks) / info.num_cpus;
       }
       machineUtilization[machine] = utilization;
   }
  
   // Adjust CPU frequencies after task completion
   adjustCPUFrequencies();
}


void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id) {
   //  Update tracking maps after migration
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
  
   // Adjust CPU frequencies after migration
   adjustCPUFrequencies();
}


void Scheduler::Shutdown(Time_t time) {
   // Wake up all machines before shutdown to avoid errors
   for (MachineId_t machine : machines) {
       MachineInfo_t info = Machine_GetInfo(machine);
       if (info.s_state != S0) {
           Machine_SetState(machine, S0);
       }
   }
  
   // Shut down all VMs
   for (auto &vm : vms) {
       VM_Shutdown(vm);
   }
  
   // Clear the tracking maps
   machinesByCPU.clear();
   vmsByType.clear();
   vmsByMachine.clear();
  
   SimOutput("SimulationComplete(): Finished!", 4);
   SimOutput("SimulationComplete(): Time is " + std::to_string(time), 4);
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
   // When memory is overcommitted, try to free up memory
   scheduler.PeriodicCheck(time);
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
   // Check if this machine was powered on from standby or powered off state
   MachineInfo_t info = Machine_GetInfo(machine_id);
  
   if (info.s_state == S0 && scheduler.IsMachineWakingUp(machine_id)) {
       // Machine is now fully powered on
       scheduler.MachineWakeUpComplete(machine_id);
   }
  
   // After a machine state change completes, check if we need to assign pending tasks
   scheduler.PeriodicCheck(time);
}


void SLAWarning(Time_t time, TaskId_t task_id) {
   // When an SLA warning is received, increase the task's priority
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
              
               SimOutput("Set machine " + std::to_string(machine) +
                        " to maximum frequency due to SLA warning", 2);
              
               break;
           }
       }
   }
}
