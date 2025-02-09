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

    task_rdd_17 = simulate_task(base_rdd, '17', 8)
    task_rdd_8 = simulate_task(base_rdd, '8', 4)
    task_rdd_3 = simulate_task(base_rdd, '3', 2)
    task_rdd_5 = simulate_task(base_rdd, '5', 2)
    task_rdd_11 = simulate_task(base_rdd, '11', 40)
    task_rdd_1 = simulate_task(base_rdd, '1', 86)
    task_rdd_6 = simulate_task(base_rdd, '6', 181)

    union_rdd_2 = spark.sparkContext.union([task_rdd_1])
    task_rdd_2 = simulate_task(union_rdd_2, '2', 2)

    union_rdd_10 = spark.sparkContext.union([task_rdd_6])
    task_rdd_10 = simulate_task(union_rdd_10, '10', 10)

    union_rdd_18 = spark.sparkContext.union([task_rdd_8, task_rdd_17])
    task_rdd_18 = simulate_task(union_rdd_18, '18', 252)

    union_rdd_7 = spark.sparkContext.union([task_rdd_11])
    task_rdd_7 = simulate_task(union_rdd_7, '7', 14)

    union_rdd_4 = spark.sparkContext.union([])
    task_rdd_4 = simulate_task(union_rdd_4, '4', 1)

    union_rdd_9 = spark.sparkContext.union([task_rdd_2, task_rdd_3, task_rdd_4])
    task_rdd_9 = simulate_task(union_rdd_9, '9', 1)

    union_rdd_13 = spark.sparkContext.union([task_rdd_9])
    task_rdd_13 = simulate_task(union_rdd_13, '13', 127)

    union_rdd_14 = spark.sparkContext.union([task_rdd_8, task_rdd_10, task_rdd_13])
    task_rdd_14 = simulate_task(union_rdd_14, '14', 5)

    union_rdd_15 = spark.sparkContext.union([task_rdd_10, task_rdd_14])
    task_rdd_15 = simulate_task(union_rdd_15, '15', 6)

    combined_rdd = spark.sparkContext.union([task_rdd_3, task_rdd_8, task_rdd_10, task_rdd_15])
    task_rdd = simulate_task(combined_rdd, 'J16_3_8_10_15', 24)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
