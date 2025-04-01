//
//  Scheduler.cpp
//  CloudSim
//
//  Created by ELMOOTAZBELLAH ELNOZAHY on 10/20/24.
//

#include "Scheduler.hpp"
#include <algorithm>
#include <climits>

// Fixed limits for number of machines
const unsigned MAX_RUNNING = 12;     // Max machines in running tier
const unsigned MIN_RUNNING = 8;      // Min machines in running tier
const unsigned STANDBY_SIZE = 4;     // Target number of standby machines

void Scheduler::Init() {
    SimOutput("Scheduler::Init(): Initializing simplified E-Eco scheduler", 1);
    
    // Start with a fixed number of active machines
    unsigned active_count = 0;
    
    // Get total machines
    unsigned total = 0;
    try {
        total = Machine_GetTotal();
    } catch (...) {
        SimOutput("Error getting total machines", 0);
        return;
    }
    
    // Put first MAX_RUNNING machines in running tier
    for (unsigned i = 0; i < total && active_count < MAX_RUNNING; i++) {
        MachineId_t id = MachineId_t(i);
        
        try {
            // Get CPU type
            CPUType_t cpu = Machine_GetCPUType(id);
            
            // Add to running tier
            running_tier.push_back(id);
            active_count++;
            
            // Create VM
            VMType_t vm_type = LINUX;
            if (cpu == POWER) {
                vm_type = AIX;
            }
            
            VMId_t vm = VM_Create(vm_type, cpu);
            VM_Attach(vm, id);
            vms.push_back(vm);
            
            SimOutput("Created VM on machine " + to_string(id), 2);
        } catch (...) {
            SimOutput("Error initializing machine " + to_string(i), 0);
        }
    }
    
    // Put next STANDBY_SIZE machines in standby tier
    for (unsigned i = active_count; i < total && i < active_count + STANDBY_SIZE; i++) {
        MachineId_t id = MachineId_t(i);
        
        try {
            // Add to standby tier
            standby_tier.push_back(id);
            
            // Set to standby state
            Machine_SetState(id, S1);
            
            SimOutput("Added machine " + to_string(id) + " to standby tier", 3);
        } catch (...) {
            SimOutput("Error setting machine " + to_string(i) + " to standby", 0);
        }
    }
    
    // All remaining machines go to off tier
    for (unsigned i = active_count + standby_tier.size(); i < total; i++) {
        MachineId_t id = MachineId_t(i);
        
        try {
            // Add to off tier
            off_tier.push_back(id);
            
            // Power off
            Machine_SetState(id, S5);
            
            SimOutput("Added machine " + to_string(id) + " to off tier", 3);
        } catch (...) {
            SimOutput("Error powering off machine " + to_string(i), 0);
        }
    }
    
    SimOutput("Initialized with " + to_string(running_tier.size()) + 
             " running, " + to_string(standby_tier.size()) + 
             " standby, and " + to_string(off_tier.size()) + 
             " off machines", 1);
}

void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id) {
    // Remove VM from migrating set
    migrating_vms.erase(vm_id);
}

void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
    // Get task requirements
    CPUType_t required_cpu;
    VMType_t required_vm;
    SLAType_t sla;
    
    try {
        required_cpu = RequiredCPUType(task_id);
        required_vm = RequiredVMType(task_id);
        sla = RequiredSLA(task_id);
    } catch (...) {
        SimOutput("Error getting task requirements", 0);
        return;
    }
    
    // Set priority based on SLA
    Priority_t priority;
    if (sla == SLA0) {
        priority = HIGH_PRIORITY;
    } else if (sla == SLA1) {
        priority = MID_PRIORITY;
    } else {
        priority = LOW_PRIORITY;
    }
    
    // Find a compatible VM with the lowest load
    VMId_t best_vm = UINT_MAX;
    unsigned lowest_tasks = UINT_MAX;
    
    for (auto vm_id : vms) {
        // Skip migrating VMs
        if (migrating_vms.find(vm_id) != migrating_vms.end()) {
            continue;
        }
        
        try {
            VMInfo_t info = VM_GetInfo(vm_id);
            
            // Check CPU and VM type
            if (info.cpu == required_cpu && info.vm_type == required_vm && 
                info.active_tasks.size() < lowest_tasks) {
                lowest_tasks = info.active_tasks.size();
                best_vm = vm_id;
            }
        } catch (...) {
            continue;
        }
    }
    
    // If found a compatible VM, use it
    if (best_vm != UINT_MAX) {
        try {
            VM_AddTask(best_vm, task_id, priority);
            SimOutput("Placed task " + to_string(task_id) + " on VM " + to_string(best_vm), 2);
            return;
        } catch (...) {
            SimOutput("Error adding task to VM", 0);
        }
    }
    
    // If no VM found, try any VM with the right CPU
    for (auto vm_id : vms) {
        // Skip migrating VMs
        if (migrating_vms.find(vm_id) != migrating_vms.end()) {
            continue;
        }
        
        try {
            VMInfo_t info = VM_GetInfo(vm_id);
            
            if (info.cpu == required_cpu) {
                VM_AddTask(vm_id, task_id, priority);
                SimOutput("Placed task " + to_string(task_id) + " on compatible VM " + to_string(vm_id), 2);
                return;
            }
        } catch (...) {
            continue;
        }
    }
    
    // lines 186 to 239 assisted by ChatGPT
    // If CPU type not found in running VMs, activate a standby machine
    if (!standby_tier.empty()) {
        // Find a machine with the right CPU
        for (auto it = standby_tier.begin(); it != standby_tier.end(); ) {
            MachineId_t machine_id = *it;
            
            try {
                CPUType_t cpu = Machine_GetCPUType(machine_id);
                
                if (cpu == required_cpu) {
                    // Move to running tier
                    it = standby_tier.erase(it);
                    running_tier.push_back(machine_id);
                    
                    // Activate
                    Machine_SetState(machine_id, S0);
                    
                    // Create VM
                    VMId_t vm_id = VM_Create(required_vm, required_cpu);
                    VM_Attach(vm_id, machine_id);
                    vms.push_back(vm_id);
                    
                    // Add task
                    VM_AddTask(vm_id, task_id, priority);
                    
                    SimOutput("Activated standby machine " + to_string(machine_id) + 
                             " for task " + to_string(task_id), 2);
                    
                    // Refill standby tier if needed
                    if (standby_tier.size() < STANDBY_SIZE / 2 && !off_tier.empty()) {
                        MachineId_t off_id = off_tier.front();
                        off_tier.erase(off_tier.begin());
                        
                        try {
                            // Wake to standby
                            Machine_SetState(off_id, S1);
                            standby_tier.push_back(off_id);
                            
                            SimOutput("Moved machine " + to_string(off_id) + 
                                     " from off to standby", 3);
                        } catch (...) {
                            SimOutput("Error waking machine " + to_string(off_id), 0);
                        }
                    }
                    
                    return;
                } else {
                    ++it;
                }
            } catch (...) {
                ++it;
            }
        }
    }
    
    // Last resort: try any VM
    for (auto vm_id : vms) {
        if (migrating_vms.find(vm_id) != migrating_vms.end()) {
            continue;
        }
        
        try {
            VM_AddTask(vm_id, task_id, HIGH_PRIORITY); // Use high priority
            SimOutput("Emergency placement of task " + to_string(task_id) + 
                     " on VM " + to_string(vm_id), 1);
            return;
        } catch (...) {
            continue;
        }
    }
    
    SimOutput("Failed to place task " + to_string(task_id), 0);
}

void Scheduler::PeriodicCheck(Time_t now) {
    // Set P-states based on utilization
    for (auto machine_id : running_tier) {
        try {
            MachineInfo_t info = Machine_GetInfo(machine_id);
            double utilization = static_cast<double>(info.active_tasks) / info.num_cpus;
            
            // Set P-state
            CPUPerformance_t p_state;
            if (utilization > 0.7) {
                p_state = P0;
            } else if (utilization > 0.4) {
                p_state = P1;
            } else if (utilization > 0.2) {
                p_state = P2;
            } else {
                p_state = P3;
            }
            
            // Apply to all cores
            for (unsigned i = 0; i < info.num_cpus; i++) {
                Machine_SetCorePerformance(machine_id, i, p_state);
            }
        } catch (...) {
            continue;
        }
    }
}

void Scheduler::TaskComplete(Time_t now, TaskId_t task_id) {
    // Check SLA
    try {
        if (IsSLAViolation(task_id)) {
            SimOutput("Task " + to_string(task_id) + " violated its SLA", 1);
        }
    } catch (...) {
        // Ignore errors
    }
}

void Scheduler::HandleMemoryWarning(MachineId_t machine_id) {
    SimOutput("Memory warning for machine " + to_string(machine_id), 1);
    
    // Find VMs on this machine
    for (auto vm_id : vms) {
        // Skip migrating VMs
        if (migrating_vms.find(vm_id) != migrating_vms.end()) {
            continue;
        }
        
        try {
            VMInfo_t info = VM_GetInfo(vm_id);
            
            // Skip if not on this machine
            if (info.machine_id != machine_id) {
                continue;
            }
            
            // Find a destination machine
            for (auto dest_id : running_tier) {
                // Skip source machine
                if (dest_id == machine_id) {
                    continue;
                }
                
                try {
                    MachineInfo_t dest_info = Machine_GetInfo(dest_id);
                    
                    // Check CPU compatibility
                    if (dest_info.cpu != info.cpu) {
                        continue;
                    }
                    
                    // Check memory availability (very conservative)
                    if (dest_info.memory_used > dest_info.memory_size / 2) {
                        continue;
                    }
                    
                    // Migrate VM
                    VM_Migrate(vm_id, dest_id);
                    migrating_vms.insert(vm_id);
                    
                    SimOutput("Migrating VM " + to_string(vm_id) + 
                             " from machine " + to_string(machine_id) + 
                             " to " + to_string(dest_id), 1);
                    return; // Only migrate one VM at a time
                } catch (...) {
                    continue;
                }
            }
        } catch (...) {
            continue;
        }
    }
    
    SimOutput("Unable to handle memory warning", 0);
}

void Scheduler::Shutdown(Time_t time) {
    // Shutdown all VMs
    vector<VMId_t> vm_copy = vms; // Copy to avoid iterator issues
    
    for (auto vm_id : vm_copy) {
        try {
            // Skip migrating VMs
            if (migrating_vms.find(vm_id) != migrating_vms.end()) {
                continue;
            }
            
            VM_Shutdown(vm_id);
        } catch (...) {
            // Ignore errors
        }
    }
}

// Public interface
static Scheduler scheduler;

void InitScheduler() {
    SimOutput("InitScheduler(): Initializing E-Eco scheduler", 4);
    scheduler.Init();
}

void HandleNewTask(Time_t time, TaskId_t task_id) {
    SimOutput("HandleNewTask(): New task " + to_string(task_id) + " at " + to_string(time), 4);
    scheduler.NewTask(time, task_id);
}

void HandleTaskCompletion(Time_t time, TaskId_t task_id) {
    SimOutput("HandleTaskCompletion(): Task " + to_string(task_id) + " completed at " + to_string(time), 4);
    scheduler.TaskComplete(time, task_id);
}

void MemoryWarning(Time_t time, MachineId_t machine_id) {
    SimOutput("MemoryWarning(): Overflow at " + to_string(machine_id) + " at " + to_string(time), 0);
    scheduler.HandleMemoryWarning(machine_id);
}

void MigrationDone(Time_t time, VMId_t vm_id) {
    SimOutput("MigrationDone(): VM " + to_string(vm_id) + " migration completed at " + to_string(time), 4);
    scheduler.MigrationComplete(time, vm_id);
}

void SchedulerCheck(Time_t time) {
    SimOutput("SchedulerCheck(): Check at " + to_string(time), 4);
    scheduler.PeriodicCheck(time);
}

void SimulationComplete(Time_t time) {
    cout << "SLA violation report" << endl;
    cout << "SLA0: " << GetSLAReport(SLA0) << "%" << endl;
    cout << "SLA1: " << GetSLAReport(SLA1) << "%" << endl;
    cout << "SLA2: " << GetSLAReport(SLA2) << "%" << endl;
    cout << "Total Energy " << Machine_GetClusterEnergy() << "KW-Hour" << endl;
    cout << "Simulation run finished in " << double(time)/1000000 << " seconds" << endl;
    SimOutput("SimulationComplete(): Finished at " + to_string(time), 4);
    
    scheduler.Shutdown(time);
}

void SLAWarning(Time_t time, TaskId_t task_id) {
    SimOutput("SLAWarning(): SLA violation for task " + to_string(task_id) + " at " + to_string(time), 1);
    
    try {
        SetTaskPriority(task_id, HIGH_PRIORITY);
    } catch (...) {
        // Ignore errors
    }
}

void StateChangeComplete(Time_t time, MachineId_t machine_id) {
    SimOutput("StateChangeComplete(): Machine " + to_string(machine_id) + " state change at " + to_string(time), 3);
}