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

    union_rdd_120 = spark.sparkContext.union([task_rdd_132])
    task_rdd_120 = simulate_task(union_rdd_120, '120', 4)

    union_rdd_98 = spark.sparkContext.union([task_rdd_120])
    task_rdd_98 = simulate_task(union_rdd_98, '98', 4)

    union_rdd_86 = spark.sparkContext.union([task_rdd_98])
    task_rdd_86 = simulate_task(union_rdd_86, '86', 2)

    combined_rdd = spark.sparkContext.union([task_rdd_86])
    task_rdd = simulate_task(combined_rdd, 'R85_86', 11)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
