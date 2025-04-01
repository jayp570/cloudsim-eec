//
//  Scheduler.cpp
//  CloudSim
//
//  Created by ELMOOTAZBELLAH ELNOZAHY on 10/20/24.
//

#include "Scheduler.hpp"
#include <vector>
#include <unordered_map>
#include <random>


static bool migrating = false;
static unsigned active_machines = 16;
std::unordered_map<VMId_t, bool> isMigrating;

vector<MachineId_t> x86Machines;
vector<MachineId_t> x86On;
vector<MachineId_t> x86Off;

vector<MachineId_t> armMachines;
vector<MachineId_t> armOn;
vector<MachineId_t> armOff;

vector<MachineId_t> powerMachines;
vector<MachineId_t> powerOn;
vector<MachineId_t> powerOff;

vector<MachineId_t> riscvMachines;
vector<MachineId_t> riscvOn;
vector<MachineId_t> riscvOff;


void Scheduler::Init() {
    // Find the parameters of the clusters
    // Get the total number of machines
    // For each machine:
    //      Get the type of the machine
    //      Get the memory of the machine
    //      Get the number of CPUs
    //      Get if there is a GPU or not
    // 
    SimOutput("Scheduler::Init(): Total number of machines is " + to_string(Machine_GetTotal()), 3);
    SimOutput("Scheduler::Init(): Initializing scheduler", 1);

    // sort out all the machines of different cpu types
    for(unsigned i = 0; i < Machine_GetTotal(); i++) {
        if (i < active_machines) {
            Machine_SetState(MachineId_t(i), S0); // turn on only essential machines
        } else {
            Machine_SetState(MachineId_t(i), S5); // keep others off initially
        }
        
        switch (Machine_GetCPUType(MachineId_t(i))) {
            case X86: x86Machines.push_back(MachineId_t(i)); break;
            case ARM: armMachines.push_back(MachineId_t(i)); break;
            case POWER: powerMachines.push_back(MachineId_t(i)); break;
            case RISCV: riscvMachines.push_back(MachineId_t(i)); break;
        }
    }
}

void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id) {
    // Update your data structure. The VM now can receive new tasks
    isMigrating[vm_id] = false;
}

void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
    TaskInfo_t taskInfo = GetTaskInfo(task_id);
    CPUType_t cpu = taskInfo.required_cpu;
    vector<MachineId_t> *machineList;

    switch (cpu) {
        case X86: machineList = &x86Machines; break;
        case ARM: machineList = &armMachines; break;
        case POWER: machineList = &powerMachines; break;
        case RISCV: machineList = &riscvMachines; break;
    }

    MachineId_t bestMachine = -1;
    double minLoad = 1.1;

    // find a machine to put this task on - choose the most available
    for (MachineId_t machine : *machineList) {
        MachineInfo_t info = Machine_GetInfo(machine);
        if (info.s_state == S0 && (info.memory_size - info.memory_used) >= taskInfo.required_memory) {
            double load = (double)info.active_tasks / info.num_cpus;
            if (load < minLoad) {
                minLoad = load;
                bestMachine = machine;
            }
        }
    }

    if (bestMachine == -1) {
        for (MachineId_t machine : *machineList) {
            if (Machine_GetInfo(machine).s_state == S5) {
                Machine_SetState(machine, S0);
                bestMachine = machine;
                break;
            }
        }
    }

    if (bestMachine != -1) {
        VMId_t vm = VM_Create(taskInfo.required_vm, cpu);
        isMigrating[vm] = false;
        VM_Attach(vm, bestMachine);
        VM_AddTask(vm, task_id, taskInfo.priority);
    } else {
        SimOutput("Task " + to_string(task_id) + " delayed due to lack of resources", 1);
    }
}

// Helper function to calculate machine utilization
double getUtilization(MachineId_t machineId) {
    MachineInfo_t info = Machine_GetInfo(machineId);
    return (double)info.active_tasks / info.num_cpus;
}


void Scheduler::PeriodicCheck(Time_t now) {
    // This method should be called from SchedulerCheck()
    // SchedulerCheck is called periodically by the simulator to allow you to monitor, make decisions, adjustments, etc.
    // Unlike the other invocations of the scheduler, this one doesn't report any specific event
    // Recommendation: Take advantage of this function to do some monitoring and adjustments as necessary

    vector<MachineId_t> overloadedMachines;
    vector<MachineId_t> underutilizedMachines;
    // sort machines by how much utilization theyre at for load balancing
    for (unsigned i = 0; i < Machine_GetTotal(); i++) {
        MachineId_t machine = MachineId_t(i);
        MachineInfo_t info = Machine_GetInfo(machine);
        if (info.s_state == S0) {
            double utilization = (double)info.active_tasks / info.num_cpus;
            if (utilization < 0.1) {
                underutilizedMachines.push_back(machine);
            } else if (utilization > 0.7) {
                overloadedMachines.push_back(machine);
            }
        }
    }
    // migrate vms from underutilizated to overloaded machines
    for (MachineId_t source : overloadedMachines) {
        for (MachineId_t target : underutilizedMachines) {
            if (Machine_GetCPUType(source) == Machine_GetCPUType(target)) {
                for (auto vm : vms) {
                    if (VM_GetInfo(vm).machine_id == source) {
                        isMigrating[vm] = true;
                        VM_Migrate(vm, target);
                        SimOutput("Migrating VM " + to_string(vm) + " from " + to_string(source) + " to " + to_string(target), 1);
                        break;
                    }
                }
            }
        }
    }
    // power saving
    for (MachineId_t machine : underutilizedMachines) {
        if (Machine_GetInfo(machine).active_tasks == 0) {
            Machine_SetState(machine, S5);
            SimOutput("Shutting down idle machine " + to_string(machine), 1);
        }
    }




}

void Scheduler::Shutdown(Time_t time) {
    // Do your final reporting and bookkeeping here.
    // Report about the total energy consumed
    // Report about the SLA compliance
    // Shutdown everything to be tidy :-)
    for(auto & vm: vms) {
        VM_Shutdown(vm);
    }
    SimOutput("SimulationComplete(): Finished!", 4);
    SimOutput("SimulationComplete(): Time is " + to_string(time), 4);
}

void Scheduler::TaskComplete(Time_t now, TaskId_t task_id) {
    // Do any bookkeeping necessary for the data structures
    // Decide if a machine is to be turned off, slowed down, or VMs to be migrated according to your policy
    // This is an opportunity to make any adjustments to optimize performance/energy
    SimOutput("Scheduler::TaskComplete(): Task " + to_string(task_id) + " is complete at " + to_string(now), 4);
}

// Public interface below

static Scheduler Scheduler;

void InitScheduler() {
    SimOutput("InitScheduler(): Initializing scheduler", 4);
    Scheduler.Init();
}

void HandleNewTask(Time_t time, TaskId_t task_id) {
    SimOutput("HandleNewTask(): Received new task " + to_string(task_id) + " at time " + to_string(time), 4);
    Scheduler.NewTask(time, task_id);
}

void HandleTaskCompletion(Time_t time, TaskId_t task_id) {
    SimOutput("HandleTaskCompletion(): Task " + to_string(task_id) + " completed at time " + to_string(time), 4);
    Scheduler.TaskComplete(time, task_id);
}

void MemoryWarning(Time_t time, MachineId_t machine_id) {
    // The simulator is alerting you that machine identified by machine_id is overcommitted
    SimOutput("MemoryWarning(): Overflow at " + to_string(machine_id) + " was detected at time " + to_string(time), 0);
}

void MigrationDone(Time_t time, VMId_t vm_id) {
    // The function is called on to alert you that migration is complete
    SimOutput("MigrationDone(): Migration of VM " + to_string(vm_id) + " was completed at time " + to_string(time), 4);
    Scheduler.MigrationComplete(time, vm_id);
    migrating = false;
}

void SchedulerCheck(Time_t time) {
    // This function is called periodically by the simulator, no specific event
    SimOutput("SchedulerCheck(): SchedulerCheck() called at " + to_string(time), 4);
    Scheduler.PeriodicCheck(time);
    // static unsigned counts = 0;
    // counts++;
    // if(counts == 10) {
    //     migrating = true;
    //     VM_Migrate(1, 9);
    // }
}

void SimulationComplete(Time_t time) {
    // This function is called before the simulation terminates Add whatever you feel like.
    cout << "SLA violation report" << endl;
    cout << "SLA0: " << GetSLAReport(SLA0) << "%" << endl;
    cout << "SLA1: " << GetSLAReport(SLA1) << "%" << endl;
    cout << "SLA2: " << GetSLAReport(SLA2) << "%" << endl;     // SLA3 do not have SLA violation issues
    cout << "Total Energy " << Machine_GetClusterEnergy() << "KW-Hour" << endl;
    cout << "Simulation run finished in " << double(time)/1000000 << " seconds" << endl;
    SimOutput("SimulationComplete(): Simulation finished at time " + to_string(time), 4);
    
    Scheduler.Shutdown(time);
}

void SLAWarning(Time_t time, TaskId_t task_id) {
    
}

void StateChangeComplete(Time_t time, MachineId_t machine_id) {
    // Called in response to an earlier request to change the state of a machine
}
