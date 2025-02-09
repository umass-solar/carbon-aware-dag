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

    task_rdd_2 = simulate_task(base_rdd, '2', 136)
    task_rdd_12 = simulate_task(base_rdd, '12', 49)
    task_rdd_9 = simulate_task(base_rdd, '9', 8)
    task_rdd_4 = simulate_task(base_rdd, '4', 76)
    task_rdd_17 = simulate_task(base_rdd, '17', 20)
    task_rdd_3 = simulate_task(base_rdd, '3', 4)
    task_rdd_131 = simulate_task(base_rdd, '131', 4)
    task_rdd_1 = simulate_task(base_rdd, '1', 2)
    task_rdd_5 = simulate_task(base_rdd, '5', 18)
    task_rdd_6 = simulate_task(base_rdd, '6', 57)
    task_rdd_8 = simulate_task(base_rdd, '8', 91)
    task_rdd_72 = simulate_task(base_rdd, '72', 34)
    task_rdd_39 = simulate_task(base_rdd, '39', 115)
    task_rdd_18 = simulate_task(base_rdd, '18', 70)
    task_rdd_35 = simulate_task(base_rdd, '35', 7)
    task_rdd_40 = simulate_task(base_rdd, '40', 3)
    task_rdd_26 = simulate_task(base_rdd, '26', 30)
    task_rdd_51 = simulate_task(base_rdd, '51', 4)
    task_rdd_30 = simulate_task(base_rdd, '30', 19)
    task_rdd_36 = simulate_task(base_rdd, '36', 8)
    task_rdd_28 = simulate_task(base_rdd, '28', 27)
    task_rdd_64 = simulate_task(base_rdd, '64', 287)

    union_rdd_11 = spark.sparkContext.union([task_rdd_1, task_rdd_3, task_rdd_4, task_rdd_9])
    task_rdd_11 = simulate_task(union_rdd_11, '11', 1)

    union_rdd_132 = spark.sparkContext.union([task_rdd_131])
    task_rdd_132 = simulate_task(union_rdd_132, '132', 1)

    union_rdd_47 = spark.sparkContext.union([task_rdd_72])
    task_rdd_47 = simulate_task(union_rdd_47, '47', 2)

    union_rdd_23 = spark.sparkContext.union([task_rdd_39])
    task_rdd_23 = simulate_task(union_rdd_23, '23', 24)

    union_rdd_19 = spark.sparkContext.union([task_rdd_18])
    task_rdd_19 = simulate_task(union_rdd_19, '19', 16)

    union_rdd_44 = spark.sparkContext.union([task_rdd_35])
    task_rdd_44 = simulate_task(union_rdd_44, '44', 5)

    union_rdd_27 = spark.sparkContext.union([task_rdd_26])
    task_rdd_27 = simulate_task(union_rdd_27, '27', 296)

    union_rdd_37 = spark.sparkContext.union([task_rdd_36])
    task_rdd_37 = simulate_task(union_rdd_37, '37', 183)

    union_rdd_41 = spark.sparkContext.union([task_rdd_64])
    task_rdd_41 = simulate_task(union_rdd_41, '41', 84)

    union_rdd_48 = spark.sparkContext.union([task_rdd_47])
    task_rdd_48 = simulate_task(union_rdd_48, '48', 296)

    union_rdd_24 = spark.sparkContext.union([task_rdd_23])
    task_rdd_24 = simulate_task(union_rdd_24, '24', 385)

    union_rdd_20 = spark.sparkContext.union([task_rdd_19])
    task_rdd_20 = simulate_task(union_rdd_20, '20', 42)

    union_rdd_32 = spark.sparkContext.union([task_rdd_26, task_rdd_27])
    task_rdd_32 = simulate_task(union_rdd_32, '32', 45)

    union_rdd_38 = spark.sparkContext.union([task_rdd_28, task_rdd_37])
    task_rdd_38 = simulate_task(union_rdd_38, '38', 8)

    union_rdd_49 = spark.sparkContext.union([task_rdd_41])
    task_rdd_49 = simulate_task(union_rdd_49, '49', 16)

    union_rdd_13 = spark.sparkContext.union([task_rdd_49])
    task_rdd_13 = simulate_task(union_rdd_13, '13', 6)

    union_rdd_21 = spark.sparkContext.union([task_rdd_20])
    task_rdd_21 = simulate_task(union_rdd_21, '21', 521)

    union_rdd_22 = spark.sparkContext.union([task_rdd_38])
    task_rdd_22 = simulate_task(union_rdd_22, '22', 45)

    union_rdd_14 = spark.sparkContext.union([task_rdd_2, task_rdd_5, task_rdd_8, task_rdd_12, task_rdd_13])
    task_rdd_14 = simulate_task(union_rdd_14, '14', 3)

    union_rdd_25 = spark.sparkContext.union([task_rdd_21, task_rdd_24])
    task_rdd_25 = simulate_task(union_rdd_25, '25', 6)

    union_rdd_45 = spark.sparkContext.union([task_rdd_22, task_rdd_26, task_rdd_30, task_rdd_32, task_rdd_40, task_rdd_44])
    task_rdd_45 = simulate_task(union_rdd_45, '45', 81)

    union_rdd_15 = spark.sparkContext.union([task_rdd_6, task_rdd_8, task_rdd_9, task_rdd_14])
    task_rdd_15 = simulate_task(union_rdd_15, '15', 36)

    union_rdd_46 = spark.sparkContext.union([task_rdd_45])
    task_rdd_46 = simulate_task(union_rdd_46, '46', 100)

    union_rdd_16 = spark.sparkContext.union([task_rdd_4, task_rdd_5, task_rdd_15])
    task_rdd_16 = simulate_task(union_rdd_16, '16', 4)

    union_rdd_31 = spark.sparkContext.union([task_rdd_46])
    task_rdd_31 = simulate_task(union_rdd_31, '31', 29)

    union_rdd_7 = spark.sparkContext.union([task_rdd_3, task_rdd_6, task_rdd_13, task_rdd_16])
    task_rdd_7 = simulate_task(union_rdd_7, '7', 47)

    union_rdd_10 = spark.sparkContext.union([task_rdd_4, task_rdd_7])
    task_rdd_10 = simulate_task(union_rdd_10, '10', 2)

    combined_rdd = spark.sparkContext.union([task_rdd_1, task_rdd_9, task_rdd_10])
    task_rdd = simulate_task(combined_rdd, 'J2_1_9_10', 1)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
