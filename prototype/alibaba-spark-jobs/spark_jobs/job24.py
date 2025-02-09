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

    task_rdd_119 = simulate_task(base_rdd, '119', 4)
    task_rdd_89 = simulate_task(base_rdd, '89', 2)

    union_rdd_120 = spark.sparkContext.union([task_rdd_89, task_rdd_119])
    task_rdd_120 = simulate_task(union_rdd_120, '120', 275)

    combined_rdd = spark.sparkContext.union([task_rdd_120])
    task_rdd = simulate_task(combined_rdd, 'R20_120', 4)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
