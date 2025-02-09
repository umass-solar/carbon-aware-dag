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

    task_rdd_9 = simulate_task(base_rdd, '9', 8)
    task_rdd_4 = simulate_task(base_rdd, '4', 92)
    task_rdd_3 = simulate_task(base_rdd, '3', 42)
    task_rdd_2 = simulate_task(base_rdd, '2', 5)
    task_rdd_1 = simulate_task(base_rdd, '1', 2)
    task_rdd_10 = simulate_task(base_rdd, '10', 25)
    task_rdd_63 = simulate_task(base_rdd, '63', 116)
    task_rdd_11 = simulate_task(base_rdd, '11', 16)
    task_rdd_18 = simulate_task(base_rdd, '18', 13)
    task_rdd_37 = simulate_task(base_rdd, '37', 11)

    union_rdd_6 = spark.sparkContext.union([task_rdd_1, task_rdd_2, task_rdd_3, task_rdd_4, task_rdd_9])
    task_rdd_6 = simulate_task(union_rdd_6, '6', 36)

    union_rdd_8 = spark.sparkContext.union([task_rdd_4])
    task_rdd_8 = simulate_task(union_rdd_8, '8', 56)

    union_rdd_13 = spark.sparkContext.union([task_rdd_2])
    task_rdd_13 = simulate_task(union_rdd_13, '13', 5)

    union_rdd_34 = spark.sparkContext.union([task_rdd_63])
    task_rdd_34 = simulate_task(union_rdd_34, '34', 2)

    union_rdd_17 = spark.sparkContext.union([task_rdd_11])
    task_rdd_17 = simulate_task(union_rdd_17, '17', 9)

    union_rdd_19 = spark.sparkContext.union([task_rdd_18])
    task_rdd_19 = simulate_task(union_rdd_19, '19', 236)

    union_rdd_30 = spark.sparkContext.union([task_rdd_37])
    task_rdd_30 = simulate_task(union_rdd_30, '30', 6)

    union_rdd_7 = spark.sparkContext.union([task_rdd_3, task_rdd_4, task_rdd_6])
    task_rdd_7 = simulate_task(union_rdd_7, '7', 25)

    union_rdd_14 = spark.sparkContext.union([task_rdd_17, task_rdd_34])
    task_rdd_14 = simulate_task(union_rdd_14, '14', 3)

    union_rdd_39 = spark.sparkContext.union([task_rdd_19])
    task_rdd_39 = simulate_task(union_rdd_39, '39', 5)

    union_rdd_21 = spark.sparkContext.union([task_rdd_9, task_rdd_10, task_rdd_11])
    task_rdd_21 = simulate_task(union_rdd_21, '21', 228)

    union_rdd_16 = spark.sparkContext.union([task_rdd_11, task_rdd_14])
    task_rdd_16 = simulate_task(union_rdd_16, '16', 2)

    union_rdd_5 = spark.sparkContext.union([task_rdd_21])
    task_rdd_5 = simulate_task(union_rdd_5, '5', 93)

    combined_rdd = spark.sparkContext.union([task_rdd_5])
    task_rdd = simulate_task(combined_rdd, 'M12_5', 9)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
