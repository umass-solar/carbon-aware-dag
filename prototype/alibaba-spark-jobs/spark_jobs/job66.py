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

    task_rdd_115 = simulate_task(base_rdd, '115', 14)
    task_rdd_131 = simulate_task(base_rdd, '131', 1)

    union_rdd_132 = spark.sparkContext.union([task_rdd_131])
    task_rdd_132 = simulate_task(union_rdd_132, '132', 1)

    union_rdd_92 = spark.sparkContext.union([task_rdd_132])
    task_rdd_92 = simulate_task(union_rdd_92, '92', 4)

    union_rdd_116 = spark.sparkContext.union([task_rdd_92, task_rdd_115])
    task_rdd_116 = simulate_task(union_rdd_116, '116', 234)

    union_rdd_120 = spark.sparkContext.union([task_rdd_116])
    task_rdd_120 = simulate_task(union_rdd_120, '120', 72)

    union_rdd_42 = spark.sparkContext.union([task_rdd_120])
    task_rdd_42 = simulate_task(union_rdd_42, '42', 4)

    union_rdd_2 = spark.sparkContext.union([task_rdd_42])
    task_rdd_2 = simulate_task(union_rdd_2, '2', 182)

    combined_rdd = spark.sparkContext.union([task_rdd_2])
    task_rdd = simulate_task(combined_rdd, 'M7_2', 2)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
