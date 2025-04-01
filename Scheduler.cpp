//
//  Scheduler.cpp
//  CloudSim
//
//  Created by ELMOOTAZBELLAH ELNOZAHY on 10/20/24.
//

//
//  Scheduler.cpp - Greedy Allocation Implementation
//  CloudSim
//

#include "Scheduler.hpp"
#include <algorithm>  // For sorting
#include <map>

// Helper function to calculate machine utilization
double getUtilization(MachineId_t machineId) {
    MachineInfo_t info = Machine_GetInfo(machineId);
    return (double)info.active_tasks / info.num_cpus;
}

// Add these declarations to Scheduler.hpp
// private:
//    vector<VMId_t> vms;
//    vector<MachineId_t> machines;
//    map<VMId_t, bool> activeVMs;
//    map<MachineId_t, vector<TaskId_t>> pendingTasks;
//    bool migrationInProgress = false;
//    
//    void TryConsolidation(Time_t now);
//    void HandleStateChange(Time_t time, MachineId_t machine_id);

// Global scheduler instance
static Scheduler scheduler;
static bool migrationInProgress = false;
static map<MachineId_t, vector<TaskId_t>> pendingTasks;

void Scheduler::Init() {
    SimOutput("Scheduler::Init(): Initializing Greedy Allocation scheduler", 1);
    
    // Get total number of machines
    unsigned totalMachines = Machine_GetTotal();
    SimOutput("Scheduler::Init(): Total number of machines is " + to_string(totalMachines), 3);
    
    // Store all machine IDs for tracking
    for(unsigned i = 0; i < totalMachines; i++) {
        machines.push_back(MachineId_t(i));
    }
    
    // Create VMs for X86 machines and store them
    for(unsigned i = 0; i < 16; i++) {
        if(Machine_GetCPUType(MachineId_t(i)) == X86) {
            VMId_t vm = VM_Create(LINUX, X86);
            vms.push_back(vm);
            VM_Attach(vm, MachineId_t(i));
        }
    }
    
    // Set all ARM machines to sleep mode initially
    for(unsigned i = 16; i < totalMachines; i++) {
        Machine_SetState(MachineId_t(i), S5);
    }
}

void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
    // Get task requirements
    CPUType_t requiredCPU = RequiredCPUType(task_id);
    unsigned memoryNeeded = GetTaskMemory(task_id);
    SLAType_t slaType = RequiredSLA(task_id);
    
    // Determine priority based on SLA
    Priority_t priority;
    if(slaType == SLA0) priority = HIGH_PRIORITY;
    else if(slaType == SLA1) priority = MID_PRIORITY;
    else priority = LOW_PRIORITY;
    
    // Find suitable machines (correct CPU type, powered on)
    vector<MachineId_t> suitableMachines;
    for(unsigned i = 0; i < Machine_GetTotal(); i++) {
        MachineId_t machineId = MachineId_t(i);
        MachineInfo_t info = Machine_GetInfo(machineId);
        
        if(Machine_GetCPUType(machineId) == requiredCPU && 
           info.s_state == S0 && 
           info.memory_size - info.memory_used >= memoryNeeded) {
            suitableMachines.push_back(machineId);
        }
    }
    
    // Sort machines by utilization (ascending)
    sort(suitableMachines.begin(), suitableMachines.end(), 
         [](MachineId_t a, MachineId_t b) {
             MachineInfo_t infoA = Machine_GetInfo(a);
             MachineInfo_t infoB = Machine_GetInfo(b);
             double utilA = (double)infoA.active_tasks / infoA.num_cpus;
             double utilB = (double)infoB.active_tasks / infoB.num_cpus;
             return utilA < utilB;
         });
    
    // Try to assign to existing VM on least loaded machine
    bool assigned = false;
    for(auto machineId : suitableMachines) {
        for(auto vmId : vms) {
            VMInfo_t vmInfo = VM_GetInfo(vmId);
            if(vmInfo.machine_id == machineId && vmInfo.cpu == requiredCPU) {
                VM_AddTask(vmId, task_id, priority);
                assigned = true;
                SimOutput("Assigned task " + to_string(task_id) + " to VM " + 
                          to_string(vmId) + " on machine " + to_string(machineId), 2);
                break;
            }
        }
        if(assigned) break;
    }
    
    // If no suitable VM found, create a new one on least loaded machine
    if(!assigned && !suitableMachines.empty()) {
        VMId_t newVm = VM_Create(RequiredVMType(task_id), requiredCPU);
        VM_Attach(newVm, suitableMachines[0]);
        VM_AddTask(newVm, task_id, priority);
        vms.push_back(newVm);
        assigned = true;
        SimOutput("Created new VM " + to_string(newVm) + " for task " + 
                  to_string(task_id) + " on machine " + to_string(suitableMachines[0]), 2);
    }
    
    // SLA Violation, wake up a sleeping machine
    if(!assigned) {
        for(unsigned i = 0; i < Machine_GetTotal(); i++) {
            MachineId_t machineId = MachineId_t(i);
            if(Machine_GetCPUType(machineId) == requiredCPU && 
               Machine_GetInfo(machineId).s_state == S5) {
                // Wake up this machine
                Machine_SetState(machineId, S0);
                pendingTasks[machineId].push_back(task_id);
                assigned = true;
                SimOutput("Waking up machine " + to_string(machineId) + 
                          " for task " + to_string(task_id), 1);
                break;
            }
        }
    }
    
    if(!assigned) {
        SimOutput("WARNING: Could not assign task " + to_string(task_id), 0);
    }
}

void Scheduler::TryConsolidation(Time_t now) {
    if(migrationInProgress) return;
    
    // Get all active machines
    vector<MachineId_t> activeMachines;
    for(unsigned i = 0; i < Machine_GetTotal(); i++) {
        MachineInfo_t info = Machine_GetInfo(MachineId_t(i));
        if(info.s_state == S0) {
            activeMachines.push_back(MachineId_t(i));
        }
    }
    
    // Sort by utilization (ascending)
    sort(activeMachines.begin(), activeMachines.end(), 
         [](MachineId_t a, MachineId_t b) {
             MachineInfo_t infoA = Machine_GetInfo(a);
             MachineInfo_t infoB = Machine_GetInfo(b);
             double utilA = (double)infoA.active_tasks / infoA.num_cpus;
             double utilB = (double)infoB.active_tasks / infoB.num_cpus;
             return utilA < utilB;
         });
    
    // If we have at least one under-utilized machine and one machine that could take its load
    if(activeMachines.size() > 1) {
        MachineId_t sourceMachine = activeMachines[0];
        MachineInfo_t sourceInfo = Machine_GetInfo(sourceMachine);
        
        // Only consolidate if machine is under-utilized
        if(sourceInfo.active_tasks > 0 && 
           (double)sourceInfo.active_tasks / sourceInfo.num_cpus < 0.3) {
            
            // Find all VMs on this machine
            vector<VMId_t> vmsToMigrate;
            for(auto vmId : vms) {
                VMInfo_t vmInfo = VM_GetInfo(vmId);
                if(vmInfo.machine_id == sourceMachine) {
                    vmsToMigrate.push_back(vmId);
                }
            }
            
            // Try to migrate each VM to another machine
            for(auto vmId : vmsToMigrate) {
                // Find destination machine
                for(size_t i = activeMachines.size() - 1; i > 0; i--) {
                    MachineId_t destMachine = activeMachines[i];
                    
                    // Check if compatible
                    if(Machine_GetCPUType(destMachine) == 
                       Machine_GetCPUType(sourceMachine)) {
                        migrationInProgress = true;
                        VM_Migrate(vmId, destMachine);
                        SimOutput("Migrating VM " + to_string(vmId) + 
                                 " from machine " + to_string(sourceMachine) + 
                                 " to machine " + to_string(destMachine), 1);
                        return; // Only do one migration at a time
                    }
                }
            }
        }
    }
}

void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id) {
    SimOutput("Scheduler::MigrationComplete(): VM " + to_string(vm_id) + " migration completed", 2);
    migrationInProgress = false;
}

void Scheduler::TaskComplete(Time_t now, TaskId_t task_id) {
    SimOutput("Scheduler::TaskComplete(): Task " + to_string(task_id) + " completed", 3);
    
    // Try consolidation after task completion
    TryConsolidation(now);
}

void Scheduler::PeriodicCheck(Time_t now) {
    // First check for tasks that are violating or at risk of violating SLAs
    for (auto vm : vms) {
        VMInfo_t vmInfo = VM_GetInfo(vm);
        for (auto taskId : vmInfo.active_tasks) {
            if (IsSLAViolation(taskId)) {
                // This task is already violating its SLA, take immediate action
                SetTaskPriority(taskId, HIGH_PRIORITY);
                
                // Look for a less loaded machine to potentially migrate to
                MachineId_t currentMachine = vmInfo.machine_id;
                
                // Find a machine with better performance (lower utilization)
                for (auto machineId : machines) {
                    if (machineId != currentMachine && 
                        Machine_GetCPUType(machineId) == Machine_GetCPUType(currentMachine) &&
                        getUtilization(machineId) < getUtilization(currentMachine) * 0.7) {
                        
                        // Found a better machine, migrate if not already migrating
                        if (!migrationInProgress) {
                            migrationInProgress = true;
                            VM_Migrate(vm, machineId);
                            SimOutput("PeriodicCheck: Migrating VM " + to_string(vm) + 
                                     " with SLA-violating task to less loaded machine " + 
                                     to_string(machineId), 1);
                            return; // Only handle one migration at a time
                        }
                    }
                }
            }
        }
    }
    
    // Then proceed with the normal consolidation logic if no SLA issues needed immediate handling
    TryConsolidation(now);
}

void Scheduler::HandleStateChange(Time_t time, MachineId_t machine_id) {
    MachineInfo_t info = Machine_GetInfo(machine_id);
    
    // If machine is now awake, process any pending tasks
    if(info.s_state == S0 && pendingTasks.count(machine_id) > 0) {
        auto tasks = pendingTasks[machine_id];
        pendingTasks.erase(machine_id);
        
        for(auto taskId : tasks) {
            if(!IsTaskCompleted(taskId)) {
                // Create VM for this task
                VMId_t newVM = VM_Create(RequiredVMType(taskId), RequiredCPUType(taskId));
                VM_Attach(newVM, machine_id);
                
                // Set priority based on SLA
                Priority_t priority;
                SLAType_t slaType = RequiredSLA(taskId);
                if(slaType == SLA0) {
                    priority = HIGH_PRIORITY;
                } else if(slaType == SLA1) {
                    priority = MID_PRIORITY;
                } else {
                    priority = LOW_PRIORITY;
                }
                
                VM_AddTask(newVM, taskId, priority);
                vms.push_back(newVM);
                SimOutput("Scheduler::HandleStateChange(): Created VM " + to_string(newVM) + 
                          " for pending task " + to_string(taskId) + " on awakened machine " + 
                          to_string(machine_id), 2);
            }
        }
    }
}

void Scheduler::Shutdown(Time_t time) {
    // Shutdown all VMs
    for(auto vm : vms) {
        VM_Shutdown(vm);
    }
    
    // Report final statistics
    double totalEnergy = Machine_GetClusterEnergy();
    SimOutput("Scheduler::Shutdown(): Total energy consumed: " + to_string(totalEnergy) + " KW-Hour", 1);
}


void InitScheduler() {
    SimOutput("InitScheduler(): Initializing scheduler", 4);
    scheduler.Init();
}

void HandleNewTask(Time_t time, TaskId_t task_id) {
    SimOutput("HandleNewTask(): Received new task " + to_string(task_id) + " at time " + to_string(time), 4);
    scheduler.NewTask(time, task_id);
}

void HandleTaskCompletion(Time_t time, TaskId_t task_id) {
    SimOutput("HandleTaskCompletion(): Task " + to_string(task_id) + " completed at time " + to_string(time), 4);
    scheduler.TaskComplete(time, task_id);
}

void MemoryWarning(Time_t time, MachineId_t machine_id) {
    SimOutput("MemoryWarning(): Overflow at " + to_string(machine_id) + " was detected at time " + to_string(time), 0);
}

void MigrationDone(Time_t time, VMId_t vm_id) {
    SimOutput("MigrationDone(): Migration of VM " + to_string(vm_id) + " was completed at time " + to_string(time), 4);
    scheduler.MigrationComplete(time, vm_id);
    migrationInProgress = false;
}

void SchedulerCheck(Time_t time) {
    SimOutput("SchedulerCheck(): SchedulerCheck() called at " + to_string(time), 4);
    scheduler.PeriodicCheck(time);
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

void SLAWarning(Time_t time, TaskId_t task_id) {
    SimOutput("SLAWarning(): SLA violation for task " + to_string(task_id) + " at time " + to_string(time), 0);
}

void StateChangeComplete(Time_t time, MachineId_t machine_id) {
    SimOutput("StateChangeComplete(): Machine " + to_string(machine_id) + " state change completed at time " + to_string(time), 3);
    scheduler.HandleStateChange(time, machine_id);
}