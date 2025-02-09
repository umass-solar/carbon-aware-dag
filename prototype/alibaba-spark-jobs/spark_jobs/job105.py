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

    task_rdd_9 = simulate_task(base_rdd, '9', 130)
    task_rdd_2 = simulate_task(base_rdd, '2', 285)
    task_rdd_8 = simulate_task(base_rdd, '8', 184)
    task_rdd_1 = simulate_task(base_rdd, '1', 5)
    task_rdd_4 = simulate_task(base_rdd, '4', 40)
    task_rdd_6 = simulate_task(base_rdd, '6', 11)
    task_rdd_5 = simulate_task(base_rdd, '5', 31)
    task_rdd_16 = simulate_task(base_rdd, '16', 1)
    task_rdd_14 = simulate_task(base_rdd, '14', 59)
    task_rdd_3 = simulate_task(base_rdd, '3', 31)
    task_rdd_18 = simulate_task(base_rdd, '18', 12)
    task_rdd_46 = simulate_task(base_rdd, '46', 153)
    task_rdd_39 = simulate_task(base_rdd, '39', 22)
    task_rdd_35 = simulate_task(base_rdd, '35', 45)
    task_rdd_27 = simulate_task(base_rdd, '27', 4)
    task_rdd_26 = simulate_task(base_rdd, '26', 5)
    task_rdd_21 = simulate_task(base_rdd, '21', 10)
    task_rdd_17 = simulate_task(base_rdd, '17', 1)
    task_rdd_29 = simulate_task(base_rdd, '29', 24)
    task_rdd_31 = simulate_task(base_rdd, '31', 4)
    task_rdd_15 = simulate_task(base_rdd, '15', 3)
    task_rdd_43 = simulate_task(base_rdd, '43', 21)
    task_rdd_73 = simulate_task(base_rdd, '73', 1)
    task_rdd_22 = simulate_task(base_rdd, '22', 10)
    task_rdd_32 = simulate_task(base_rdd, '32', 12)
    task_rdd_38 = simulate_task(base_rdd, '38', 5)
    task_rdd_30 = simulate_task(base_rdd, '30', 68)

    union_rdd_11 = spark.sparkContext.union([task_rdd_6, task_rdd_8])
    task_rdd_11 = simulate_task(union_rdd_11, '11', 3)

    union_rdd_20 = spark.sparkContext.union([task_rdd_17, task_rdd_18])
    task_rdd_20 = simulate_task(union_rdd_20, '20', 26)

    union_rdd_50 = spark.sparkContext.union([])
    task_rdd_50 = simulate_task(union_rdd_50, '50', 58)

    union_rdd_34 = spark.sparkContext.union([task_rdd_17])
    task_rdd_34 = simulate_task(union_rdd_34, '34', 31)

    union_rdd_12 = spark.sparkContext.union([task_rdd_3, task_rdd_6, task_rdd_8, task_rdd_9, task_rdd_11])
    task_rdd_12 = simulate_task(union_rdd_12, '12', 67)

    union_rdd_47 = spark.sparkContext.union([task_rdd_50])
    task_rdd_47 = simulate_task(union_rdd_47, '47', 406)

    union_rdd_36 = spark.sparkContext.union([task_rdd_21, task_rdd_34, task_rdd_35])
    task_rdd_36 = simulate_task(union_rdd_36, '36', 84)

    union_rdd_69 = spark.sparkContext.union([task_rdd_47])
    task_rdd_69 = simulate_task(union_rdd_69, '69', 192)

    union_rdd_37 = spark.sparkContext.union([task_rdd_26, task_rdd_27, task_rdd_36])
    task_rdd_37 = simulate_task(union_rdd_37, '37', 56)

    union_rdd_45 = spark.sparkContext.union([task_rdd_69])
    task_rdd_45 = simulate_task(union_rdd_45, '45', 21)

    union_rdd_40 = spark.sparkContext.union([task_rdd_29, task_rdd_37, task_rdd_39])
    task_rdd_40 = simulate_task(union_rdd_40, '40', 329)

    union_rdd_33 = spark.sparkContext.union([task_rdd_32, task_rdd_35, task_rdd_45])
    task_rdd_33 = simulate_task(union_rdd_33, '33', 638)

    union_rdd_42 = spark.sparkContext.union([task_rdd_15, task_rdd_21, task_rdd_27, task_rdd_31, task_rdd_35, task_rdd_40])
    task_rdd_42 = simulate_task(union_rdd_42, '42', 1077)

    union_rdd_23 = spark.sparkContext.union([task_rdd_33])
    task_rdd_23 = simulate_task(union_rdd_23, '23', 1)

    union_rdd_48 = spark.sparkContext.union([task_rdd_42])
    task_rdd_48 = simulate_task(union_rdd_48, '48', 10)

    union_rdd_24 = spark.sparkContext.union([task_rdd_22, task_rdd_23])
    task_rdd_24 = simulate_task(union_rdd_24, '24', 41)

    union_rdd_49 = spark.sparkContext.union([task_rdd_48])
    task_rdd_49 = simulate_task(union_rdd_49, '49', 191)

    combined_rdd = spark.sparkContext.union([task_rdd_5, task_rdd_8, task_rdd_12])
    task_rdd = simulate_task(combined_rdd, 'R13_5_8_12', 20)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
