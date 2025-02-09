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

    task_rdd_14 = simulate_task(base_rdd, '14', 40)
    task_rdd_10 = simulate_task(base_rdd, '10', 5)
    task_rdd_7 = simulate_task(base_rdd, '7', 2)
    task_rdd_3 = simulate_task(base_rdd, '3', 38)
    task_rdd_115 = simulate_task(base_rdd, '115', 14)
    task_rdd_4 = simulate_task(base_rdd, '4', 37)
    task_rdd_2 = simulate_task(base_rdd, '2', 16)
    task_rdd_21 = simulate_task(base_rdd, '21', 347)
    task_rdd_6 = simulate_task(base_rdd, '6', 1)
    task_rdd_31 = simulate_task(base_rdd, '31', 9)
    task_rdd_47 = simulate_task(base_rdd, '47', 32)
    task_rdd_30 = simulate_task(base_rdd, '30', 68)

    union_rdd_16 = spark.sparkContext.union([task_rdd_47])
    task_rdd_16 = simulate_task(union_rdd_16, '16', 2)

    union_rdd_15 = spark.sparkContext.union([task_rdd_3, task_rdd_7, task_rdd_14])
    task_rdd_15 = simulate_task(union_rdd_15, '15', 3)

    union_rdd_13 = spark.sparkContext.union([task_rdd_3])
    task_rdd_13 = simulate_task(union_rdd_13, '13', 29)

    union_rdd_116 = spark.sparkContext.union([task_rdd_115])
    task_rdd_116 = simulate_task(union_rdd_116, '116', 985)

    union_rdd_5 = spark.sparkContext.union([task_rdd_30])
    task_rdd_5 = simulate_task(union_rdd_5, '5', 4)

    union_rdd_1 = spark.sparkContext.union([task_rdd_2])
    task_rdd_1 = simulate_task(union_rdd_1, '1', 1)

    union_rdd_9 = spark.sparkContext.union([task_rdd_6])
    task_rdd_9 = simulate_task(union_rdd_9, '9', 13)

    union_rdd_32 = spark.sparkContext.union([task_rdd_31])
    task_rdd_32 = simulate_task(union_rdd_32, '32', 16)

    union_rdd_17 = spark.sparkContext.union([task_rdd_7, task_rdd_16])
    task_rdd_17 = simulate_task(union_rdd_17, '17', 8)

    union_rdd_120 = spark.sparkContext.union([task_rdd_116])
    task_rdd_120 = simulate_task(union_rdd_120, '120', 72)

    union_rdd_8 = spark.sparkContext.union([task_rdd_4, task_rdd_5])
    task_rdd_8 = simulate_task(union_rdd_8, '8', 1)

    union_rdd_12 = spark.sparkContext.union([task_rdd_9])
    task_rdd_12 = simulate_task(union_rdd_12, '12', 4)

    union_rdd_53 = spark.sparkContext.union([])
    task_rdd_53 = simulate_task(union_rdd_53, '53', 6)

    union_rdd_20 = spark.sparkContext.union([task_rdd_53])
    task_rdd_20 = simulate_task(union_rdd_20, '20', 5)

    union_rdd_11 = spark.sparkContext.union([task_rdd_20])
    task_rdd_11 = simulate_task(union_rdd_11, '11', 42)

    combined_rdd = spark.sparkContext.union([task_rdd_5])
    task_rdd = simulate_task(combined_rdd, 'R19_5_18', 3)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
