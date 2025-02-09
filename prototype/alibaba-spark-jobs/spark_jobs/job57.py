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

    task_rdd_7 = simulate_task(base_rdd, '7', 6)
    task_rdd_21 = simulate_task(base_rdd, '21', 21)
    task_rdd_6 = simulate_task(base_rdd, '6', 3)
    task_rdd_37 = simulate_task(base_rdd, '37', 96)
    task_rdd_17 = simulate_task(base_rdd, '17', 5)
    task_rdd_11 = simulate_task(base_rdd, '11', 54)
    task_rdd_2 = simulate_task(base_rdd, '2', 19)
    task_rdd_5 = simulate_task(base_rdd, '5', 5)
    task_rdd_1 = simulate_task(base_rdd, '1', 22)
    task_rdd_25 = simulate_task(base_rdd, '25', 117)
    task_rdd_24 = simulate_task(base_rdd, '24', 27)
    task_rdd_30 = simulate_task(base_rdd, '30', 70)
    task_rdd_20 = simulate_task(base_rdd, '20', 4)

    union_rdd_3 = spark.sparkContext.union([task_rdd_2, task_rdd_6, task_rdd_7])
    task_rdd_3 = simulate_task(union_rdd_3, '3', 5)

    union_rdd_22 = spark.sparkContext.union([task_rdd_6, task_rdd_21])
    task_rdd_22 = simulate_task(union_rdd_22, '22', 6)

    union_rdd_29 = spark.sparkContext.union([task_rdd_37])
    task_rdd_29 = simulate_task(union_rdd_29, '29', 1)

    union_rdd_10 = spark.sparkContext.union([task_rdd_1, task_rdd_5])
    task_rdd_10 = simulate_task(union_rdd_10, '10', 15)

    union_rdd_16 = spark.sparkContext.union([task_rdd_11])
    task_rdd_16 = simulate_task(union_rdd_16, '16', 64)

    union_rdd_26 = spark.sparkContext.union([task_rdd_25])
    task_rdd_26 = simulate_task(union_rdd_26, '26', 36)

    union_rdd_28 = spark.sparkContext.union([task_rdd_20])
    task_rdd_28 = simulate_task(union_rdd_28, '28', 106)

    union_rdd_8 = spark.sparkContext.union([task_rdd_3])
    task_rdd_8 = simulate_task(union_rdd_8, '8', 1)

    union_rdd_18 = spark.sparkContext.union([task_rdd_17, task_rdd_29])
    task_rdd_18 = simulate_task(union_rdd_18, '18', 1271)

    union_rdd_14 = spark.sparkContext.union([task_rdd_10])
    task_rdd_14 = simulate_task(union_rdd_14, '14', 83)

    union_rdd_27 = spark.sparkContext.union([task_rdd_26])
    task_rdd_27 = simulate_task(union_rdd_27, '27', 6)

    union_rdd_31 = spark.sparkContext.union([task_rdd_28])
    task_rdd_31 = simulate_task(union_rdd_31, '31', 3)

    union_rdd_9 = spark.sparkContext.union([task_rdd_1, task_rdd_2, task_rdd_3, task_rdd_8])
    task_rdd_9 = simulate_task(union_rdd_9, '9', 4)

    union_rdd_35 = spark.sparkContext.union([task_rdd_25, task_rdd_27])
    task_rdd_35 = simulate_task(union_rdd_35, '35', 9)

    union_rdd_4 = spark.sparkContext.union([task_rdd_2, task_rdd_9])
    task_rdd_4 = simulate_task(union_rdd_4, '4', 74)

    union_rdd_36 = spark.sparkContext.union([task_rdd_35])
    task_rdd_36 = simulate_task(union_rdd_36, '36', 192)

    union_rdd_34 = spark.sparkContext.union([task_rdd_30])
    task_rdd_34 = simulate_task(union_rdd_34, '34', 2)

    union_rdd_39 = spark.sparkContext.union([])
    task_rdd_39 = simulate_task(union_rdd_39, '39', 5)

    union_rdd_32 = spark.sparkContext.union([task_rdd_31, task_rdd_34])
    task_rdd_32 = simulate_task(union_rdd_32, '32', 103)

    union_rdd_40 = spark.sparkContext.union([task_rdd_39])
    task_rdd_40 = simulate_task(union_rdd_40, '40', 3)

    union_rdd_12 = spark.sparkContext.union([task_rdd_40])
    task_rdd_12 = simulate_task(union_rdd_12, '12', 96)

    union_rdd_23 = spark.sparkContext.union([task_rdd_11, task_rdd_12, task_rdd_17, task_rdd_22])
    task_rdd_23 = simulate_task(union_rdd_23, '23', 12)

    union_rdd_15 = spark.sparkContext.union([task_rdd_2, task_rdd_11, task_rdd_12, task_rdd_14])
    task_rdd_15 = simulate_task(union_rdd_15, '15', 9)

    union_rdd_19 = spark.sparkContext.union([task_rdd_2, task_rdd_10, task_rdd_12, task_rdd_15, task_rdd_18])
    task_rdd_19 = simulate_task(union_rdd_19, '19', 568)

    union_rdd_13 = spark.sparkContext.union([task_rdd_12, task_rdd_19])
    task_rdd_13 = simulate_task(union_rdd_13, '13', 240)

    combined_rdd = spark.sparkContext.union([task_rdd_7, task_rdd_32])
    task_rdd = simulate_task(combined_rdd, 'J33_7_32', 8)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
