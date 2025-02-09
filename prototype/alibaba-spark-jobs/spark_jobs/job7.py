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

    task_rdd_8 = simulate_task(base_rdd, '8', 54)
    task_rdd_16 = simulate_task(base_rdd, '16', 4)
    task_rdd_56 = simulate_task(base_rdd, '56', 115)
    task_rdd_3 = simulate_task(base_rdd, '3', 25)
    task_rdd_30 = simulate_task(base_rdd, '30', 463)
    task_rdd_4 = simulate_task(base_rdd, '4', 6)
    task_rdd_6 = simulate_task(base_rdd, '6', 8)
    task_rdd_7 = simulate_task(base_rdd, '7', 28)
    task_rdd_5 = simulate_task(base_rdd, '5', 56)
    task_rdd_2 = simulate_task(base_rdd, '2', 5)
    task_rdd_26 = simulate_task(base_rdd, '26', 26)
    task_rdd_20 = simulate_task(base_rdd, '20', 18)
    task_rdd_13 = simulate_task(base_rdd, '13', 44)
    task_rdd_40 = simulate_task(base_rdd, '40', 11)
    task_rdd_104 = simulate_task(base_rdd, '104', 13)
    task_rdd_129 = simulate_task(base_rdd, '129', 4)
    task_rdd_99 = simulate_task(base_rdd, '99', 3)
    task_rdd_36 = simulate_task(base_rdd, '36', 24)
    task_rdd_69 = simulate_task(base_rdd, '69', 2)
    task_rdd_35 = simulate_task(base_rdd, '35', 99)
    task_rdd_47 = simulate_task(base_rdd, '47', 2)
    task_rdd_49 = simulate_task(base_rdd, '49', 9)
    task_rdd_41 = simulate_task(base_rdd, '41', 15)
    task_rdd_24 = simulate_task(base_rdd, '24', 70)
    task_rdd_18 = simulate_task(base_rdd, '18', 8)

    union_rdd_10 = spark.sparkContext.union([task_rdd_3, task_rdd_4, task_rdd_5, task_rdd_6, task_rdd_7])
    task_rdd_10 = simulate_task(union_rdd_10, '10', 3)

    union_rdd_14 = spark.sparkContext.union([])
    task_rdd_14 = simulate_task(union_rdd_14, '14', 37)

    union_rdd_53 = spark.sparkContext.union([task_rdd_2, task_rdd_6])
    task_rdd_53 = simulate_task(union_rdd_53, '53', 13)

    union_rdd_131 = spark.sparkContext.union([])
    task_rdd_131 = simulate_task(union_rdd_131, '131', 3)

    union_rdd_98 = spark.sparkContext.union([])
    task_rdd_98 = simulate_task(union_rdd_98, '98', 14)

    union_rdd_71 = spark.sparkContext.union([])
    task_rdd_71 = simulate_task(union_rdd_71, '71', 8)

    union_rdd_23 = spark.sparkContext.union([task_rdd_18])
    task_rdd_23 = simulate_task(union_rdd_23, '23', 3)

    union_rdd_11 = spark.sparkContext.union([task_rdd_2, task_rdd_3, task_rdd_4, task_rdd_10])
    task_rdd_11 = simulate_task(union_rdd_11, '11', 2)

    union_rdd_15 = spark.sparkContext.union([task_rdd_14, task_rdd_56])
    task_rdd_15 = simulate_task(union_rdd_15, '15', 4)

    union_rdd_44 = spark.sparkContext.union([task_rdd_53])
    task_rdd_44 = simulate_task(union_rdd_44, '44', 168)

    union_rdd_132 = spark.sparkContext.union([task_rdd_131])
    task_rdd_132 = simulate_task(union_rdd_132, '132', 1)

    union_rdd_72 = spark.sparkContext.union([task_rdd_71])
    task_rdd_72 = simulate_task(union_rdd_72, '72', 26)

    union_rdd_42 = spark.sparkContext.union([task_rdd_23, task_rdd_41])
    task_rdd_42 = simulate_task(union_rdd_42, '42', 16)

    union_rdd_12 = spark.sparkContext.union([task_rdd_3, task_rdd_11])
    task_rdd_12 = simulate_task(union_rdd_12, '12', 10)

    union_rdd_101 = spark.sparkContext.union([task_rdd_132])
    task_rdd_101 = simulate_task(union_rdd_101, '101', 4)

    union_rdd_73 = spark.sparkContext.union([task_rdd_72])
    task_rdd_73 = simulate_task(union_rdd_73, '73', 16)

    union_rdd_43 = spark.sparkContext.union([task_rdd_42])
    task_rdd_43 = simulate_task(union_rdd_43, '43', 256)

    union_rdd_50 = spark.sparkContext.union([task_rdd_43, task_rdd_49])
    task_rdd_50 = simulate_task(union_rdd_50, '50', 72)

    union_rdd_48 = spark.sparkContext.union([task_rdd_50])
    task_rdd_48 = simulate_task(union_rdd_48, '48', 4)

    union_rdd_27 = spark.sparkContext.union([task_rdd_48, task_rdd_73, task_rdd_98, task_rdd_101, task_rdd_104])
    task_rdd_27 = simulate_task(union_rdd_27, '27', 22)

    union_rdd_28 = spark.sparkContext.union([task_rdd_27])
    task_rdd_28 = simulate_task(union_rdd_28, '28', 18)

    union_rdd_31 = spark.sparkContext.union([task_rdd_5, task_rdd_13, task_rdd_20, task_rdd_28, task_rdd_30])
    task_rdd_31 = simulate_task(union_rdd_31, '31', 214)

    union_rdd_32 = spark.sparkContext.union([task_rdd_2, task_rdd_4, task_rdd_27, task_rdd_31])
    task_rdd_32 = simulate_task(union_rdd_32, '32', 63)

    union_rdd_33 = spark.sparkContext.union([task_rdd_4, task_rdd_32])
    task_rdd_33 = simulate_task(union_rdd_33, '33', 20)

    union_rdd_1 = spark.sparkContext.union([task_rdd_30, task_rdd_33])
    task_rdd_1 = simulate_task(union_rdd_1, '1', 10)

    combined_rdd = spark.sparkContext.union([task_rdd_3, task_rdd_6, task_rdd_8, task_rdd_12])
    task_rdd = simulate_task(combined_rdd, 'J21_3_6_8_12', 7)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
