import argparse
import json
import logging
import random

import simpy


from . import ComputeNode, Job, MasterNode, plot_gantt_chart, Tracker

logger = logging.getLogger(__name__)

def generate_jobs(env, master, config):
    """Generate jobs, each with multiple tasks with the same duration and a related dataset
    The number of tasks in each job, the tasks duration, and the dataset size are all selected
    from a uniform distribution between a Min and Max values."""
    for i in range(config['total_nb_jobs']):
        job_id = i
        nb_tasks = random.randint(config['min_nb_tasks_per_job'], config['max_nb_tasks_per_job'])
        task_duration = random.uniform(config['min_task_duration_sec'], config['max_task_duration_sec'])
        dataset_size = random.uniform(config['min_dataset_size_MB'], config['max_dataset_size_MB'])
        yield master.queue.put(Job(job_id, task_duration, nb_tasks, dataset_size))
        inter_arrival_time = random.expovariate(config['jobs_inter_arrival_expovariate'])
        yield env.timeout(inter_arrival_time)  # Simulate job inter-arrivals


def configure_logging(log_level):
    """Configure the logger format."""
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def run_simulation(config):
    """Run the discrete-event based simulation with specified config"""
    env = simpy.Environment()
    tracker = Tracker(env)
    master = MasterNode(env, [], tracker, config)
    compute_nodes = [ComputeNode(env, i, master, config['compute_node_bw_MBps' ])
                     for i in range(config['total_nb_compute_nodes'])]
    master.compute_nodes = compute_nodes

    # Start all SimPy processes
    env.process(master.submit_jobs())
    if config['reschedule']:
        env.process(master.reschedule())  # Enable rescheduling at runtime

    for node in compute_nodes:
        env.process(node.process_tasks())

    env.process(generate_jobs(env, master, config))
    env.run()

    return tracker


# Example on how to extract basic statistics about simulation results
def get_jobs_mean_processing_time(results):
    total_job_duration = 0
    for job in results.tasks_duration_per_job.values():
        job_duration = job['end_time'] - job['start_time']
        total_job_duration += job_duration
    return total_job_duration/len(results.tasks_duration_per_job)


def main():
    parser = argparse.ArgumentParser(description="Simulate task scheduling.")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level (default: INFO).",
    )
    parser.add_argument(
        "--config",
        help="Specify the path of the config file",
    )
    parser.add_argument(
        "--plot-gantt",
        action="store_true",
        help="Plot the gantt chart of the simulation",
    )
    args = parser.parse_args()

    # Initialize logging
    configure_logging(args.log_level)

    # Specify a seed to perform multiple runs with the same generated jobs for comparison
    random.seed(42)

    with open(args.config, "r", encoding="utf-8") as f:
        config = json.load(f)

    # Run the simulation
    logger.info("Simulation begins with config: %s" ,str(config))
    results = run_simulation(config=config)

    # Print some metrics
    logger.info("total wall time: %s", str(results.total_wall_time))
    logger.info("total transferred bytes: %s", str(results.total_nb_transferred_bytes))
    mean_job_processing_time = get_jobs_mean_processing_time(results)
    logger.info("mean job processing time (end_time - submission_time): %s", str(mean_job_processing_time))

    # Plot gantt chart if required. This is obvisouly for small runs only.
    if args.plot_gantt:
        plot_gantt_chart(task_data=results.events_history, num_nodes=config['total_nb_compute_nodes'])


if __name__ == "__main__":
    main()
