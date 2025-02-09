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

    task_rdd_29 = simulate_task(base_rdd, '29', 2)

    combined_rdd = spark.sparkContext.union([task_rdd_29])
    task_rdd = simulate_task(combined_rdd, 'J30_29', 86)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
