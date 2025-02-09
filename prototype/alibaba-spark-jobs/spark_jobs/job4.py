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

    task_rdd_83 = simulate_task(base_rdd, '83', 17)
    task_rdd_132 = simulate_task(base_rdd, '132', 4)
    task_rdd_117 = simulate_task(base_rdd, '117', 1)
    task_rdd_114 = simulate_task(base_rdd, '114', 4)
    task_rdd_89 = simulate_task(base_rdd, '89', 2)

    union_rdd_82 = spark.sparkContext.union([task_rdd_132])
    task_rdd_82 = simulate_task(union_rdd_82, '82', 4)

    union_rdd_118 = spark.sparkContext.union([task_rdd_114, task_rdd_117])
    task_rdd_118 = simulate_task(union_rdd_118, '118', 46)

    union_rdd_119 = spark.sparkContext.union([task_rdd_118])
    task_rdd_119 = simulate_task(union_rdd_119, '119', 2)

    union_rdd_120 = spark.sparkContext.union([task_rdd_89, task_rdd_119])
    task_rdd_120 = simulate_task(union_rdd_120, '120', 275)

    union_rdd_79 = spark.sparkContext.union([task_rdd_120])
    task_rdd_79 = simulate_task(union_rdd_79, '79', 4)

    union_rdd_63 = spark.sparkContext.union([task_rdd_79])
    task_rdd_63 = simulate_task(union_rdd_63, '63', 358)

    combined_rdd = spark.sparkContext.union([task_rdd_63, task_rdd_82, task_rdd_83])
    task_rdd = simulate_task(combined_rdd, 'J84_63_82_83', 7)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
