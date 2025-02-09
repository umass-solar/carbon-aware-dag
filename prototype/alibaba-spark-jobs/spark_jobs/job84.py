from pyspark.sql import SparkSession
import time

def simulate_task(rdd, task_name, duration):
    def simulate(_):
        print(f'Starting task: {task_name}')
        time.sleep(duration / 60)
        print(f'Completed task: {task_name}')
        return task_name
    return rdd.map(simulate)

if __name__ == '__main__':
    spark = SparkSession.builder.appName('Task Simulation').getOrCreate()

    base_rdd = spark.sparkContext.parallelize(['base'])

    task_rdd_99 = simulate_task(base_rdd, '99', 103)

    combined_rdd = spark.sparkContext.union([task_rdd_99])
    task_rdd = simulate_task(combined_rdd, 'R9_99', 10)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
