//
//  Scheduler.cpp
//  CloudSim
//
//  Created by ELMOOTAZBELLAH ELNOZAHY on 10/20/24.
//

#include "Scheduler.hpp"
#include <algorithm>
#include <climits>

void Scheduler::Init() {
    SimOutput("Scheduler::Init(): Initializing minimal pMapper scheduler", 1);
    
    // Set a reasonable limit on active machines
    unsigned max_active = 16;
    unsigned x86_count = 0, arm_count = 0, power_count = 0, riscv_count = 0;
    
    // Get total number of machines
    unsigned total_machines = 0;
    try {
        total_machines = Machine_GetTotal();
    } catch (...) {
        SimOutput("Error getting total machines", 0);
        return;
    }
    
    // lines 29 through 73 assisted by ChatGPT
    // Create VMs on a limited number of machines
    for (unsigned i = 0; i < total_machines && machines.size() < max_active; i++) {
        try {
            MachineId_t machine_id = MachineId_t(i);
            CPUType_t cpu_type = Machine_GetCPUType(machine_id);
            
            // Limit by CPU type
            bool create_vm = false;
            
            if (cpu_type == X86 && x86_count < 8) {
                create_vm = true;
                x86_count++;
            } else if (cpu_type == ARM && arm_count < 4) {
                create_vm = true;
                arm_count++;
            } else if (cpu_type == POWER && power_count < 2) {
                create_vm = true;
                power_count++;
            } else if (cpu_type == RISCV && riscv_count < 2) {
                create_vm = true;
                riscv_count++;
            }
            
            if (create_vm) {
                // Create appropriate VM type
                VMType_t vm_type = LINUX;
                if (cpu_type == POWER) {
                    vm_type = AIX;
                }
                
                VMId_t vm_id = VM_Create(vm_type, cpu_type);
                VM_Attach(vm_id, machine_id);
                
                vms.push_back(vm_id);
                machines.push_back(machine_id);
                
                SimOutput("Created VM " + to_string(vm_id) + " on machine " + to_string(machine_id), 2);
            } else {
                // Power off unused machines
                Machine_SetState(machine_id, S5);
            }
        } catch (...) {
            continue;
        }
    }
    
    SimOutput("Scheduler::Init(): Created " + to_string(vms.size()) + " VMs", 1);
}

void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id) {
    // Update our tracking
    migrating_vms.erase(vm_id);
    SimOutput("Migration complete for VM " + to_string(vm_id), 2);
}

void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
    // Get task requirements
    CPUType_t required_cpu;
    VMType_t required_vm;
    SLAType_t sla_type;
    
    try {
        required_cpu = RequiredCPUType(task_id);
        required_vm = RequiredVMType(task_id);
        sla_type = RequiredSLA(task_id);
    } catch (...) {
        SimOutput("Error getting task requirements", 0);
        return;
    }
    
    // Set priority based on SLA
    Priority_t priority;
    if (sla_type == SLA0) {
        priority = HIGH_PRIORITY;
    } else if (sla_type == SLA1) {
        priority = MID_PRIORITY;
    } else {
        priority = LOW_PRIORITY;
    }
    
    // Simple placement: find least loaded VM of the right type
    VMId_t best_vm = UINT_MAX;
    unsigned least_tasks = UINT_MAX;
    
    // lines 114 to 131 assisted by ChatGPT
    for (auto vm_id : vms) {
        if (migrating_vms.find(vm_id) != migrating_vms.end()) {
            continue;
        }
        
        try {
            VMInfo_t info = VM_GetInfo(vm_id);
            
            // Check requirements
            if (info.cpu == required_cpu && info.vm_type == required_vm && 
                info.active_tasks.size() < least_tasks) {
                least_tasks = info.active_tasks.size();
                best_vm = vm_id;
            }
        } catch (...) {
            continue;
        }
    }
    
    // Try to place on best VM
    if (best_vm != UINT_MAX) {
        try {
            VM_AddTask(best_vm, task_id, priority);
            SimOutput("Placed task " + to_string(task_id) + " on VM " + to_string(best_vm), 2);
            return;
        } catch (...) {
            // Placement failed, continue to next strategy
        }
    }
    
    // Try any VM with right CPU type if optimal placement failed
    for (auto vm_id : vms) {
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
    
    // Last resort: try any VM
    for (auto vm_id : vms) {
        // Skip migrating VMs
        if (migrating_vms.find(vm_id) != migrating_vms.end()) {
            continue;
        }
        
        try {
            VM_AddTask(vm_id, task_id, HIGH_PRIORITY); // Use high priority for emergency
            SimOutput("Emergency placement of task " + to_string(task_id) + " on VM " + to_string(vm_id), 1);
            return;
        } catch (...) {
            // Continue trying other VMs
        }
    }
    
    SimOutput("Failed to place task " + to_string(task_id), 0);
}

void Scheduler::PeriodicCheck(Time_t now) {
    // Simple P-state adjustment based on load
    for (auto machine_id : machines) {
        try {
            MachineInfo_t info = Machine_GetInfo(machine_id);
            
            // Skip powered off machines
            if (info.s_state == S5) {
                continue;
            }
            
            // Calculate utilization
            double utilization = static_cast<double>(info.active_tasks) / info.num_cpus;
            
            // Set P-state based on utilization
            CPUPerformance_t p_state;
            if (utilization > 0.7) {
                p_state = P0;  // High load
            } else if (utilization > 0.4) {
                p_state = P1;  // Medium load
            } else if (utilization > 0.2) {
                p_state = P2;  // Low load
            } else {
                p_state = P3;  // Very low load
            }
            
            // Apply to all cores
            for (unsigned i = 0; i < info.num_cpus; i++) {
                Machine_SetCorePerformance(machine_id, i, p_state);
            }
        } catch (...) {
            // Skip if error
            continue;
        }
    }
}

void Scheduler::TaskComplete(Time_t now, TaskId_t task_id) {
    // Check SLA violations
    try {
        if (IsSLAViolation(task_id)) {
            SimOutput("Task " + to_string(task_id) + " violated its SLA", 1);
        }
    } catch (...) {
        // Ignore SLA check errors
    }
}

void Scheduler::HandleMemoryWarning(MachineId_t machine_id) {
    SimOutput("Handling memory warning for machine " + to_string(machine_id), 1);
    
    // Only migrate if not too many VMs are already migrating
    if (migrating_vms.size() >= 2) {
        return;
    }
    
    // Find a VM to migrate
    for (auto vm_id : vms) {
        // Skip if already migrating
        if (migrating_vms.find(vm_id) != migrating_vms.end()) {
            continue;
        }
        
        try {
            VMInfo_t info = VM_GetInfo(vm_id);
            
            // Only consider VMs on the affected machine
            if (info.machine_id != machine_id) {
                continue;
            }
            
            // Find a destination machine
            for (auto dest_id : machines) {
                // Skip source machine
                if (dest_id == machine_id) {
                    continue;
                }
                
                try {
                    // Check destination compatibility
                    MachineInfo_t dest_info = Machine_GetInfo(dest_id);
                    
                    if (dest_info.s_state != S5 && dest_info.cpu == info.cpu) {
                        // Initiate migration
                        VM_Migrate(vm_id, dest_id);
                        migrating_vms.insert(vm_id);
                        
                        SimOutput("Migrating VM " + to_string(vm_id) + " from machine " + 
                                 to_string(machine_id) + " to " + to_string(dest_id), 1);
                        return; // Only do one migration at a time
                    }
                } catch (...) {
                    continue;
                }
            }
        } catch (...) {
            continue;
        }
    }
}

void Scheduler::Shutdown(Time_t time) {
    // Safely shutdown all VMs
    vector<VMId_t> vm_copy = vms; // Make a copy to avoid iterator invalidation
    
    for (auto vm_id : vm_copy) {
        // Skip migrating VMs
        if (migrating_vms.find(vm_id) != migrating_vms.end()) {
            continue;
        }
        
        try {
            VM_Shutdown(vm_id);
        } catch (...) {
            // Ignore shutdown errors
        }
    }
    
    SimOutput("Scheduler shutdown complete", 1);
}

// Public interface functions
static Scheduler scheduler;

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
    scheduler.HandleMemoryWarning(machine_id);
}

void MigrationDone(Time_t time, VMId_t vm_id) {
    SimOutput("MigrationDone(): Migration of VM " + to_string(vm_id) + " was completed at time " + to_string(time), 4);
    scheduler.MigrationComplete(time, vm_id);
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
    SimOutput("SLAWarning(): SLA violation for task " + to_string(task_id) + " at time " + to_string(time), 1);
    
    // Increase priority
    try {
        SetTaskPriority(task_id, HIGH_PRIORITY);
    } catch (...) {
        // Ignore errors
    }
}

void StateChangeComplete(Time_t time, MachineId_t machine_id) {
    SimOutput("StateChangeComplete(): Machine " + to_string(machine_id) + " state change completed", 3);
}