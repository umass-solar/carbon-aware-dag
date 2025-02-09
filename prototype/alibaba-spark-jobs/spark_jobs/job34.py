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

    task_rdd_102 = simulate_task(base_rdd, '102', 16)
    task_rdd_120 = simulate_task(base_rdd, '120', 6)

    union_rdd_103 = spark.sparkContext.union([task_rdd_102])
    task_rdd_103 = simulate_task(union_rdd_103, '103', 2)

    union_rdd_100 = spark.sparkContext.union([task_rdd_120])
    task_rdd_100 = simulate_task(union_rdd_100, '100', 4)

    union_rdd_104 = spark.sparkContext.union([task_rdd_100, task_rdd_103])
    task_rdd_104 = simulate_task(union_rdd_104, '104', 45)

    combined_rdd = spark.sparkContext.union([task_rdd_104])
    task_rdd = simulate_task(combined_rdd, 'R107_104', 72)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
