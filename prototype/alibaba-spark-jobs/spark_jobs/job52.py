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

    task_rdd_74 = simulate_task(base_rdd, '74', 74)
    task_rdd_132 = simulate_task(base_rdd, '132', 4)

    union_rdd_99 = spark.sparkContext.union([task_rdd_74])
    task_rdd_99 = simulate_task(union_rdd_99, '99', 82)

    union_rdd_95 = spark.sparkContext.union([task_rdd_132])
    task_rdd_95 = simulate_task(union_rdd_95, '95', 4)

    union_rdd_98 = spark.sparkContext.union([task_rdd_95])
    task_rdd_98 = simulate_task(union_rdd_98, '98', 2)

    union_rdd_100 = spark.sparkContext.union([task_rdd_98, task_rdd_99])
    task_rdd_100 = simulate_task(union_rdd_100, '100', 97)

    combined_rdd = spark.sparkContext.union([task_rdd_100])
    task_rdd = simulate_task(combined_rdd, 'R90_100', 15)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
