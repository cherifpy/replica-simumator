import logging

import simpy


logger = logging.getLogger(__name__)

class ComputeNode:
    def __init__(self, env, node_id, master, bandwidth):
        self.env = env
        self.node_id = node_id
        self.queue = simpy.Store(env)
        self.master = master
        self.bandwidth = bandwidth
        self.bandwidth_lock = simpy.Resource(env, capacity=bandwidth)


    def process_tasks(self):
        while True:
            new_task = yield self.queue.get()
            logger.debug("[%s] Compute-%s: Got new task with job_id: %s, task_id: %s, duration: %s, dataset_size: %s",
                         self.env.now, self.node_id, new_task.job_id, new_task.task_id, new_task.duration, new_task.dataset_size)

            # Wait for the data transfer event to complete
            yield new_task.dataset_ready_event

            # Simulate task processing
            start_time = self.env.now
            self.master.tracker.log_task_start(new_task.job_id, new_task.task_id, self.node_id, start_time)
            yield self.env.timeout(new_task.duration)  # actual processing
            end_time = self.env.now
            self.master.tracker.log_task_end(new_task.job_id, new_task.task_id, self.node_id, start_time, end_time)
