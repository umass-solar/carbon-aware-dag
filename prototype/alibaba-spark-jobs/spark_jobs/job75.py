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

    task_rdd_1 = simulate_task(base_rdd, '1', 1)
    task_rdd_6 = simulate_task(base_rdd, '6', 8)
    task_rdd_4 = simulate_task(base_rdd, '4', 1)
    task_rdd_11 = simulate_task(base_rdd, '11', 31)
    task_rdd_2 = simulate_task(base_rdd, '2', 36)
    task_rdd_14 = simulate_task(base_rdd, '14', 82)
    task_rdd_48 = simulate_task(base_rdd, '48', 162)

    union_rdd_54 = spark.sparkContext.union([task_rdd_1])
    task_rdd_54 = simulate_task(union_rdd_54, '54', 162)

    union_rdd_9 = spark.sparkContext.union([task_rdd_4, task_rdd_6])
    task_rdd_9 = simulate_task(union_rdd_9, '9', 1795)

    union_rdd_12 = spark.sparkContext.union([task_rdd_11])
    task_rdd_12 = simulate_task(union_rdd_12, '12', 55)

    union_rdd_21 = spark.sparkContext.union([task_rdd_48])
    task_rdd_21 = simulate_task(union_rdd_21, '21', 32)

    union_rdd_16 = spark.sparkContext.union([task_rdd_54])
    task_rdd_16 = simulate_task(union_rdd_16, '16', 23)

    union_rdd_5 = spark.sparkContext.union([task_rdd_4, task_rdd_9])
    task_rdd_5 = simulate_task(union_rdd_5, '5', 50)

    union_rdd_13 = spark.sparkContext.union([task_rdd_2, task_rdd_12])
    task_rdd_13 = simulate_task(union_rdd_13, '13', 6)

    union_rdd_15 = spark.sparkContext.union([task_rdd_21])
    task_rdd_15 = simulate_task(union_rdd_15, '15', 189)

    union_rdd_10 = spark.sparkContext.union([task_rdd_16])
    task_rdd_10 = simulate_task(union_rdd_10, '10', 2)

    union_rdd_24 = spark.sparkContext.union([task_rdd_5])
    task_rdd_24 = simulate_task(union_rdd_24, '24', 10)

    union_rdd_17 = spark.sparkContext.union([task_rdd_15])
    task_rdd_17 = simulate_task(union_rdd_17, '17', 3)

    union_rdd_8 = spark.sparkContext.union([task_rdd_24])
    task_rdd_8 = simulate_task(union_rdd_8, '8', 27)

    union_rdd_3 = spark.sparkContext.union([task_rdd_2, task_rdd_6, task_rdd_8, task_rdd_10])
    task_rdd_3 = simulate_task(union_rdd_3, '3', 280)

    union_rdd_7 = spark.sparkContext.union([task_rdd_3])
    task_rdd_7 = simulate_task(union_rdd_7, '7', 99)

    union_rdd_60 = spark.sparkContext.union([task_rdd_7])
    task_rdd_60 = simulate_task(union_rdd_60, '60', 1)

    union_rdd_18 = spark.sparkContext.union([task_rdd_17, task_rdd_60])
    task_rdd_18 = simulate_task(union_rdd_18, '18', 4)

    combined_rdd = spark.sparkContext.union([task_rdd_18])
    task_rdd = simulate_task(combined_rdd, 'M25_18', 1)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
