This is NOT a round robin algorithm. When this branch was created, it was meant to be a round robin algorithm. However, we changed plans and this is now a load balanced power saving algorithm. It migrates tasks from machines that have high utilization to low utilization, balancing task loads. It also shuts off machines that do not have active tasks in order to save energy. 


This is the repository for the Cloud Simulator project for CS 378. To run this project, you can run `make all` to build the executable, which you can then run with `./simulator`.

For questions, please reach out to any of the course staff on via email (anish.palakurthi@utexas.edu, tarun.mohan@utexas.edu, mootaz@austin.utexas.edu) or Ed Discussion.