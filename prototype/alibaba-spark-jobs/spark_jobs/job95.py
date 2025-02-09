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

    task_rdd_129 = simulate_task(base_rdd, '129', 4)

    union_rdd_130 = spark.sparkContext.union([task_rdd_129])
    task_rdd_130 = simulate_task(union_rdd_130, '130', 1)

    union_rdd_131 = spark.sparkContext.union([task_rdd_130])
    task_rdd_131 = simulate_task(union_rdd_131, '131', 3)

    union_rdd_132 = spark.sparkContext.union([task_rdd_131])
    task_rdd_132 = simulate_task(union_rdd_132, '132', 1)

    union_rdd_63 = spark.sparkContext.union([task_rdd_132])
    task_rdd_63 = simulate_task(union_rdd_63, '63', 4)

    combined_rdd = spark.sparkContext.union([task_rdd_63])
    task_rdd = simulate_task(combined_rdd, 'R24_63', 6)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
