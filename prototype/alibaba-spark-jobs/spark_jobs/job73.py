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

    task_rdd_131 = simulate_task(base_rdd, '131', 4)

    union_rdd_132 = spark.sparkContext.union([task_rdd_131])
    task_rdd_132 = simulate_task(union_rdd_132, '132', 1)

    union_rdd_116 = spark.sparkContext.union([task_rdd_132])
    task_rdd_116 = simulate_task(union_rdd_116, '116', 4)

    union_rdd_120 = spark.sparkContext.union([task_rdd_116])
    task_rdd_120 = simulate_task(union_rdd_120, '120', 72)

    union_rdd_75 = spark.sparkContext.union([task_rdd_120])
    task_rdd_75 = simulate_task(union_rdd_75, '75', 4)

    combined_rdd = spark.sparkContext.union([task_rdd_75])
    task_rdd = simulate_task(combined_rdd, 'R71_75', 6)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
