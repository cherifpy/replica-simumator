class Task:
    """Task class representing a unit of work within a job."""
    def __init__(self, job_id,  task_id, duration, dataset_size):
        self.job_id = job_id
        self.task_id = task_id
        self.duration = duration
        self.dataset_size = dataset_size
        self.dataset_ready_event = None


class Job:
    """Job class representing a collection of tasks sharing the same dataset."""
    def __init__(self, job_id, tasks_duration, nb_tasks, dataset_size):
        self.job_id = job_id
        self.tasks = [Task(job_id=job_id, task_id=i, duration=tasks_duration, dataset_size=dataset_size)
                      for i in range(nb_tasks)]
        self.dataset_size = dataset_size
