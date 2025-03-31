//
//  Scheduler.hpp
//  CloudSim
//  Created by ELMOOTAZBELLAH ELNOZAHY on 10/20/24.
//


#ifndef Scheduler_hpp
#define Scheduler_hpp


#include <vector>
#include <map>
#include <set>
#include <algorithm>


#include "Interfaces.h"
#include "SimTypes.h"


class Scheduler {
public:
   const std::vector<VMId_t>& GetVMs() const { return vms; }
   const std::vector<MachineId_t>& GetMachines() const { return machines; }
   bool IsMachineActive(MachineId_t machine) const {
       return activeMachines.find(machine) != activeMachines.end();
   }
   void ActivateMachine(MachineId_t machine) {
       activeMachines.insert(machine);
       machineUtilization[machine] = 0.0;
       // Remove from waking up set if it was there
       wakingUpMachines.erase(machine);
   }
   void DeactivateMachine(MachineId_t machine) {
       activeMachines.erase(machine);
   }
  
   void AddVM(VMId_t vm) {
       vms.push_back(vm);
   }
   bool AssignTaskToVM(TaskId_t task_id, Time_t now);
  
   Scheduler() {}
   void Init();
   void MigrationComplete(Time_t time, VMId_t vm_id);
   void NewTask(Time_t now, TaskId_t task_id);
   void PeriodicCheck(Time_t now);
   void Shutdown(Time_t time);
   void TaskComplete(Time_t now, TaskId_t task_id);
  
   // Make these public so they can be accessed by the global functions
   std::map<VMType_t, std::set<VMId_t>> vmsByType;
   std::map<MachineId_t, std::set<VMId_t>> vmsByMachine;
  
   // Methods for handling machine state transitions
   void WakeUpMachine(MachineId_t machine);
   void MachineWakeUpComplete(MachineId_t machine);
   bool IsMachineWakingUp(MachineId_t machine) const {
       return wakingUpMachines.find(machine) != wakingUpMachines.end();
   }
  
private:
   // Thresholds for underload/overload detection
   const double UNDERLOAD_THRESHOLD = 0.3;  // 30% utilization
   const double OVERLOAD_THRESHOLD = 0.8;   // 80% utilization
  
   // DVFS thresholds for different P-states
   const double P0_THRESHOLD = 0.8;  // Use P0 when utilization is above 80%
   const double P1_THRESHOLD = 0.6;  // Use P1 when utilization is between 60% and 80%
   const double P2_THRESHOLD = 0.4;  // Use P2 when utilization is between 40% and 60%
                                     // Use P3 when utilization is below 40%
  
   // Track machine utilization
   std::map<MachineId_t, double> machineUtilization;
  
   // Track machine states
   std::set<MachineId_t> activeMachines;    // Machines that are fully powered on and ready
   std::set<MachineId_t> wakingUpMachines;  // Machines that are in the process of waking up
   std::set<TaskId_t> pendingTasks;
  
   // Lists of VMs and machines
   std::vector<VMId_t> vms;
   std::vector<MachineId_t> machines;


   std::map<CPUType_t, std::set<MachineId_t>> machinesByCPU;
  
   // DVFS related methods
   void adjustCPUFrequencies();
   CPUPerformance_t determineCPUFrequency(double utilization);
};


#endif /* Scheduler_hpp */
