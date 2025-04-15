# Description
This implements a simple discrete-event simulator of a distributed computing platform where clients can submit *Jobs* to a MasterNode.
These Jobs will be scheduled to be executed on a set of ComputeNodes.
A Job is composed of M *Tasks* that all have the same execution time and that share the same *Dataset*.
Tasks among the same Job can be scheduled on different ComputeNodes according to any scheduling policy.
The processing of a Task can only start when the related dataset has been totally transferred on the chosen ComputeNode.
A *Tacker* monitors what's happening and log the transfer and processing durations for post-processing.

Note that this only simulates the processing and transfer times, i.e. no real application are actually executed and no data are actually transferred.

# Usage

## Create a virtual environment and install required packages
```
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt 
```

## Configure the simulation parameters. 
There is a config_sample.json in the repo: 

    
    "total_nb_jobs": 10,
    "total_nb_compute_nodes": 6,
    "min_nb_tasks_per_job": 5,
    "max_nb_tasks_per_job": 5,
    "min_task_duration_sec": 10,
    "max_task_duration_sec": 10, 
    "min_dataset_size_MB": 10, 
    "max_dataset_size_MB": 10,
    "replication_factor": 1,
    "compute_node_bw_MBps": 1,
    "reschedule": "false",
    "jobs_inter_arrival_expovariate": 0.1


## Start a simulation
`python3 simulator.py --config config_sample.json --log-level DEBUG --plot-gantt`

## Start an experimental campaign
A toy example is given in the xp_campaign.py:
`python3 xp_campaign.py`

# Visualization: 
It is possible to draw a Gantt chart of the processing times and transfer times per node with the `plot_gantt_chart` method. 
This is obviosuly to draw toy examples as it will produce abstract art if trying to plot too many jobs/nodes.

# Assumption / Current limitations
- *Jobs* are generated randomly and interarrival times being exponentially distributed (cf config `jobs_inter_arrival_expovariate`). 
- *Tasks* all have the same execution time and share the same *Dataset*
- Transfers are *serialized*: if a transfer is currently going, the next transfer will have to wait. (i.e. we do not share the bandwidth in this first version but this could be easily implemented) 
- We assume the MasterNode bandwidth is infinite (in practice, this is the ComputeNode links that will be the bottleneck)
- All ComputeNodes have the same (configurable) bandwidth. Having heterogeneous bandwidth capacities would better reflect a real network.
- A task can be executed on any node. It would be interesting to limit the subset of candidate nodes for a given task (i.e. to emulate hardware requirements.)
- The scheduling algorithm just uses a basic 'least loaded queue' among computes nodes. This could be easily enhanced.
- The 'rescheduling' has only been implemented for a replication factor equals to one. Do not try with others (or modify the code :)# replica-simumator
