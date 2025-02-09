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

    task_rdd_29 = simulate_task(base_rdd, '29', 119)
    task_rdd_28 = simulate_task(base_rdd, '28', 2)
    task_rdd_6 = simulate_task(base_rdd, '6', 6)
    task_rdd_5 = simulate_task(base_rdd, '5', 6)
    task_rdd_23 = simulate_task(base_rdd, '23', 10)
    task_rdd_22 = simulate_task(base_rdd, '22', 68)
    task_rdd_9 = simulate_task(base_rdd, '9', 67)
    task_rdd_4 = simulate_task(base_rdd, '4', 17)
    task_rdd_3 = simulate_task(base_rdd, '3', 21)
    task_rdd_2 = simulate_task(base_rdd, '2', 4)
    task_rdd_1 = simulate_task(base_rdd, '1', 26)
    task_rdd_21 = simulate_task(base_rdd, '21', 79)
    task_rdd_20 = simulate_task(base_rdd, '20', 340)
    task_rdd_17 = simulate_task(base_rdd, '17', 36)
    task_rdd_35 = simulate_task(base_rdd, '35', 95)
    task_rdd_24 = simulate_task(base_rdd, '24', 121)
    task_rdd_14 = simulate_task(base_rdd, '14', 47)
    task_rdd_131 = simulate_task(base_rdd, '131', 4)
    task_rdd_15 = simulate_task(base_rdd, '15', 9)
    task_rdd_52 = simulate_task(base_rdd, '52', 37)
    task_rdd_25 = simulate_task(base_rdd, '25', 13)
    task_rdd_26 = simulate_task(base_rdd, '26', 3)
    task_rdd_54 = simulate_task(base_rdd, '54', 15)
    task_rdd_44 = simulate_task(base_rdd, '44', 32)
    task_rdd_48 = simulate_task(base_rdd, '48', 4)

    union_rdd_13 = spark.sparkContext.union([task_rdd_4, task_rdd_5])
    task_rdd_13 = simulate_task(union_rdd_13, '13', 276)

    union_rdd_11 = spark.sparkContext.union([task_rdd_1])
    task_rdd_11 = simulate_task(union_rdd_11, '11', 43)

    union_rdd_36 = spark.sparkContext.union([task_rdd_35])
    task_rdd_36 = simulate_task(union_rdd_36, '36', 76)

    union_rdd_51 = spark.sparkContext.union([])
    task_rdd_51 = simulate_task(union_rdd_51, '51', 4)

    union_rdd_58 = spark.sparkContext.union([])
    task_rdd_58 = simulate_task(union_rdd_58, '58', 2)

    union_rdd_27 = spark.sparkContext.union([])
    task_rdd_27 = simulate_task(union_rdd_27, '27', 7)

    union_rdd_30 = spark.sparkContext.union([task_rdd_22, task_rdd_23, task_rdd_24, task_rdd_26, task_rdd_27, task_rdd_28, task_rdd_29])
    task_rdd_30 = simulate_task(union_rdd_30, '30', 251)

    union_rdd_10 = spark.sparkContext.union([task_rdd_1, task_rdd_9, task_rdd_13])
    task_rdd_10 = simulate_task(union_rdd_10, '10', 74)

    union_rdd_16 = spark.sparkContext.union([task_rdd_51])
    task_rdd_16 = simulate_task(union_rdd_16, '16', 2)

    union_rdd_31 = spark.sparkContext.union([task_rdd_23, task_rdd_30])
    task_rdd_31 = simulate_task(union_rdd_31, '31', 125)

    union_rdd_12 = spark.sparkContext.union([task_rdd_2, task_rdd_10])
    task_rdd_12 = simulate_task(union_rdd_12, '12', 67)

    union_rdd_19 = spark.sparkContext.union([task_rdd_14, task_rdd_15, task_rdd_16, task_rdd_36])
    task_rdd_19 = simulate_task(union_rdd_19, '19', 13)

    union_rdd_32 = spark.sparkContext.union([task_rdd_26, task_rdd_27, task_rdd_28, task_rdd_31])
    task_rdd_32 = simulate_task(union_rdd_32, '32', 177)

    union_rdd_49 = spark.sparkContext.union([task_rdd_3, task_rdd_12])
    task_rdd_49 = simulate_task(union_rdd_49, '49', 5)

    combined_rdd = spark.sparkContext.union([task_rdd_21, task_rdd_22, task_rdd_25, task_rdd_28, task_rdd_29, task_rdd_32])
    task_rdd = simulate_task(combined_rdd, 'J33_21_22_25_28_29_32', 18)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
