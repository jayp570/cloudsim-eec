//
//  Scheduler.hpp
//  CloudSim
//
//  Created by ELMOOTAZBELLAH ELNOZAHY on 10/20/24.
//

#ifndef Scheduler_hpp
#define Scheduler_hpp

#include <vector>
#include <unordered_set>

#include "Interfaces.h"

class Scheduler {
public:
    Scheduler() {}
    void Init();
    void MigrationComplete(Time_t time, VMId_t vm_id);
    void NewTask(Time_t now, TaskId_t task_id);
    void PeriodicCheck(Time_t now);
    void Shutdown(Time_t now);
    void TaskComplete(Time_t now, TaskId_t task_id);
    void HandleMemoryWarning(MachineId_t machine_id);
    
private:
    // Basic tracking
    vector<VMId_t> vms;
    unordered_set<VMId_t> migrating_vms;
    
    // E-Eco three tiers - simplified
    vector<MachineId_t> running_tier;
    vector<MachineId_t> standby_tier;
    vector<MachineId_t> off_tier;
};

#endif /* Scheduler_hpp */