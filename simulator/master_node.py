import logging
import math
import random
import simpy

logger = logging.getLogger(__name__)

class MasterNode:
    """Master node handles job submissions."""
    def __init__(self, env, compute_nodes, tracker, config):
        self.env = env
        self.queue = simpy.Store(env)
        self.compute_nodes = compute_nodes
        self.tracker = tracker

        self._config = config
        self.all_jobs = {}


    def submit_jobs(self):
        """Submit a job, handling data transfer first."""
        while True:
            new_job = yield self.queue.get()
            logger.debug("[%s] Master: Got new job: %s", self.env.now, new_job)
            self.all_jobs[new_job.job_id] = {'dataset_size': new_job.dataset_size, 'nb_rescheduled': 0}
            self.tracker.register_job(new_job.job_id, self.env.now)

            # Initial scheduling
            compute_nodes = self.select_least_loaded_nodes(self._config['replication_factor'])
            tasks_per_node = max(1, math.floor(len(new_job.tasks) / self._config['replication_factor']))
            task_index = 0

            for compute_node in compute_nodes:
                # Data transfer
                dataset_ready_event = self.env.event()
                self.env.process(self.transfer_data(new_job.job_id, new_job.dataset_size, compute_node,
                                                    dataset_ready_event))

                # Submit all tasks of the job to the compute node's queue
                for task in new_job.tasks[task_index:]:
                    task.dataset_ready_event = dataset_ready_event  # Inject the ready event
                    yield compute_node.queue.put(task)
                    logger.debug("[%s] Master: Sent %s tasks to node %s", self.env.now, len(new_job.tasks),
                                 compute_node.node_id)
                    task_index += 1
                    if task_index % tasks_per_node == 0:
                        break  # Enough tasks for this node


    def transfer_data(self, job_id, dataset_size, compute_node, dataset_ready_event):
        with compute_node.bandwidth_lock.request() as node_req:
            yield node_req  # Acquire both master and node bandwidths
            transfer_time = dataset_size / compute_node.bandwidth
            start_time = self.env.now
            yield self.env.timeout(transfer_time)  # actual transfer
            end_time = self.env.now
            self.tracker.log_transfer(job_id, compute_node.node_id, start_time, end_time, dataset_size)
            dataset_ready_event.succeed()


    ########## Scheduling ##########

    def select_random_node(self):
        return random.choice(self.compute_nodes)


    def select_least_loaded_nodes(self, k):
        indexes = list(range(len(self.compute_nodes)))
        queues_load = [len(self.compute_nodes[i].queue.items) for  i in indexes]
        indexes.sort(key=lambda i: queues_load[i])
        k_least_loaded_nodes_indexes = indexes[:k]
        return [self.compute_nodes[i] for i in k_least_loaded_nodes_indexes]


    def reschedule(self):
        while True:  # while simulation_not_finished
            yield self.env.timeout(1)
            for task_id, task_info in self.tracker.ongoing_tasks.copy().items():

                elapsed = self.env.now - task_info['start_time']
                job_id = int(task_id.split("-")[0])
                transfer_time = self.all_jobs[job_id]['dataset_size'] / self.compute_nodes[task_info['node_id']].bandwidth

                # Check if processing time is now larger than transfer time
                if elapsed > transfer_time and self.all_jobs[job_id]['nb_rescheduled'] < 1:
                    logger.debug("[%s] Task %s rescheduled due to processing time exceeding transfer time.",
                                 self.env.now, task_id)
                    self.all_jobs[job_id]['nb_rescheduled'] += 1

                    current_queue = self.compute_nodes[task_info['node_id']].queue
                    remaining_tasks = [task for task in current_queue.items if task.job_id == job_id]
                    if not remaining_tasks:
                        return

                    # Remove remaining tasks from the current queue
                    nb_tasks_to_be_rescheduled = math.ceil(len(remaining_tasks)/2)
                    remaining_tasks.reverse()
                    for task in remaining_tasks[: nb_tasks_to_be_rescheduled]:
                        current_queue.items.remove(task)

                    # Select a new compute node
                    new_node = self.select_least_loaded_nodes(1)[0]
                    new_queue = new_node.queue

                    # Transfer dataset to the new node
                    dataset_size = self.all_jobs[job_id]['dataset_size']
                    dataset_ready_event = self.env.event()
                    self.env.process(self.transfer_data(job_id, dataset_size, new_node, dataset_ready_event))

                    for task in remaining_tasks[: nb_tasks_to_be_rescheduled]:
                        task.dataset_ready_event = dataset_ready_event  # Inject the ready event
                        yield new_queue.put(task)

             # Check that simulation is over
            if len(self.all_jobs) == self._config['total_nb_jobs'] and len(self.tracker.ongoing_tasks) == 0 and len(self.queue.items) == 0:
                finished = True
                for compute_node in self.compute_nodes:  # Be sure that nothing is waiting in any compute queue.
                    if len(compute_node.queue.items) > 0:
                        finished = False
                if finished:
                    break
