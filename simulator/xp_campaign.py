import json
import logging
import random

from simulator import run_simulation, get_jobs_mean_processing_time
from plot import plot_line, plot_gantt_chart

logger = logging.getLogger(__name__)

# This is an example of an experiment campagin where we make vary the replication factor.
# We then plot the mean job processing time vs the total amount of transferred bytes.
def main():
    # Specify a seed to perform multiple runs with the same generated jobs.
    random.seed(42)

    with open("config_sample.json", "r", encoding="utf-8") as f:
        config = json.load(f)

    to_plot = {config['total_nb_compute_nodes']: []}

    for replication_factor in range(1, config['total_nb_compute_nodes']):
        config['replication_factor'] = replication_factor

        # Run the actual simulation
        logger.info("Simulation begins with config: %s" ,str(config))
        results = run_simulation(config=config)

        # Uncomment if necessary
        #plot_gantt_chart(task_data=results.events_history, num_nodes=config['total_nb_compute_nodes'])

        # Some analysis
        logger.info("total wall time: %s", str(results.total_wall_time))
        logger.info("total transferred bytes: %s", str(results.total_nb_transferred_bytes))
        mean_job_processing_time = get_jobs_mean_processing_time(results)
        logger.info("mean job processing time (end_time - submission_time): %s", str(mean_job_processing_time))
        to_plot[config['total_nb_compute_nodes']].append((config['replication_factor'], mean_job_processing_time))

    plot_line(to_plot, title="Impact of the replication factor on the mean job processing time",
              xlabel="replication factor",
              ylabel="mean job processing time")


if __name__ == "__main__":
    main()
