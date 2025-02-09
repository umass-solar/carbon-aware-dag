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

    task_rdd_14 = simulate_task(base_rdd, '14', 3)
    task_rdd_5 = simulate_task(base_rdd, '5', 42)
    task_rdd_12 = simulate_task(base_rdd, '12', 164)
    task_rdd_25 = simulate_task(base_rdd, '25', 4)
    task_rdd_7 = simulate_task(base_rdd, '7', 37)
    task_rdd_40 = simulate_task(base_rdd, '40', 8)
    task_rdd_35 = simulate_task(base_rdd, '35', 37)
    task_rdd_60 = simulate_task(base_rdd, '60', 1)
    task_rdd_4 = simulate_task(base_rdd, '4', 18)
    task_rdd_2 = simulate_task(base_rdd, '2', 72)
    task_rdd_8 = simulate_task(base_rdd, '8', 39)
    task_rdd_1 = simulate_task(base_rdd, '1', 2)
    task_rdd_26 = simulate_task(base_rdd, '26', 32)
    task_rdd_20 = simulate_task(base_rdd, '20', 8)
    task_rdd_17 = simulate_task(base_rdd, '17', 4)
    task_rdd_18 = simulate_task(base_rdd, '18', 7)
    task_rdd_3 = simulate_task(base_rdd, '3', 85)

    union_rdd_10 = spark.sparkContext.union([task_rdd_1, task_rdd_5])
    task_rdd_10 = simulate_task(union_rdd_10, '10', 3)

    union_rdd_15 = spark.sparkContext.union([task_rdd_1, task_rdd_2, task_rdd_3, task_rdd_4, task_rdd_14])
    task_rdd_15 = simulate_task(union_rdd_15, '15', 17)

    union_rdd_13 = spark.sparkContext.union([task_rdd_1, task_rdd_3, task_rdd_7, task_rdd_12])
    task_rdd_13 = simulate_task(union_rdd_13, '13', 11)

    union_rdd_11 = spark.sparkContext.union([task_rdd_1, task_rdd_7, task_rdd_25])
    task_rdd_11 = simulate_task(union_rdd_11, '11', 77)

    union_rdd_34 = spark.sparkContext.union([task_rdd_60])
    task_rdd_34 = simulate_task(union_rdd_34, '34', 2)

    union_rdd_21 = spark.sparkContext.union([task_rdd_20])
    task_rdd_21 = simulate_task(union_rdd_21, '21', 2)

    union_rdd_9 = spark.sparkContext.union([task_rdd_5, task_rdd_8, task_rdd_10])
    task_rdd_9 = simulate_task(union_rdd_9, '9', 87)

    union_rdd_16 = spark.sparkContext.union([task_rdd_14, task_rdd_15])
    task_rdd_16 = simulate_task(union_rdd_16, '16', 8)

    union_rdd_41 = spark.sparkContext.union([task_rdd_34, task_rdd_35, task_rdd_40])
    task_rdd_41 = simulate_task(union_rdd_41, '41', 200)

    union_rdd_22 = spark.sparkContext.union([task_rdd_21])
    task_rdd_22 = simulate_task(union_rdd_22, '22', 414)

    union_rdd_6 = spark.sparkContext.union([task_rdd_41])
    task_rdd_6 = simulate_task(union_rdd_6, '6', 2)

    union_rdd_23 = spark.sparkContext.union([task_rdd_22])
    task_rdd_23 = simulate_task(union_rdd_23, '23', 78)

    union_rdd_24 = spark.sparkContext.union([task_rdd_17, task_rdd_23])
    task_rdd_24 = simulate_task(union_rdd_24, '24', 58)

    union_rdd_19 = spark.sparkContext.union([task_rdd_18, task_rdd_24])
    task_rdd_19 = simulate_task(union_rdd_19, '19', 51)

    union_rdd_27 = spark.sparkContext.union([task_rdd_19, task_rdd_26])
    task_rdd_27 = simulate_task(union_rdd_27, '27', 65)

    union_rdd_28 = spark.sparkContext.union([task_rdd_27])
    task_rdd_28 = simulate_task(union_rdd_28, '28', 18)

    combined_rdd = spark.sparkContext.union([task_rdd_8, task_rdd_9])
    task_rdd = simulate_task(combined_rdd, 'R14_8_9', 484)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
