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

    task_rdd_13 = simulate_task(base_rdd, '13', 5)
    task_rdd_22 = simulate_task(base_rdd, '22', 44)
    task_rdd_8 = simulate_task(base_rdd, '8', 36)
    task_rdd_4 = simulate_task(base_rdd, '4', 4)
    task_rdd_3 = simulate_task(base_rdd, '3', 6)
    task_rdd_2 = simulate_task(base_rdd, '2', 2)
    task_rdd_11 = simulate_task(base_rdd, '11', 108)
    task_rdd_10 = simulate_task(base_rdd, '10', 23)
    task_rdd_7 = simulate_task(base_rdd, '7', 7)
    task_rdd_6 = simulate_task(base_rdd, '6', 13)
    task_rdd_37 = simulate_task(base_rdd, '37', 66)
    task_rdd_26 = simulate_task(base_rdd, '26', 29)
    task_rdd_5 = simulate_task(base_rdd, '5', 27)
    task_rdd_18 = simulate_task(base_rdd, '18', 2)
    task_rdd_1 = simulate_task(base_rdd, '1', 2)
    task_rdd_15 = simulate_task(base_rdd, '15', 13)
    task_rdd_24 = simulate_task(base_rdd, '24', 18)
    task_rdd_54 = simulate_task(base_rdd, '54', 11)
    task_rdd_21 = simulate_task(base_rdd, '21', 20)
    task_rdd_29 = simulate_task(base_rdd, '29', 74)
    task_rdd_28 = simulate_task(base_rdd, '28', 17)

    union_rdd_23 = spark.sparkContext.union([task_rdd_7, task_rdd_8, task_rdd_10, task_rdd_13, task_rdd_22])
    task_rdd_23 = simulate_task(union_rdd_23, '23', 16)

    union_rdd_9 = spark.sparkContext.union([task_rdd_3, task_rdd_4, task_rdd_5, task_rdd_6, task_rdd_8])
    task_rdd_9 = simulate_task(union_rdd_9, '9', 1)

    union_rdd_17 = spark.sparkContext.union([task_rdd_1, task_rdd_2, task_rdd_3, task_rdd_4])
    task_rdd_17 = simulate_task(union_rdd_17, '17', 1)

    union_rdd_63 = spark.sparkContext.union([task_rdd_2])
    task_rdd_63 = simulate_task(union_rdd_63, '63', 116)

    union_rdd_14 = spark.sparkContext.union([task_rdd_13])
    task_rdd_14 = simulate_task(union_rdd_14, '14', 11)

    union_rdd_19 = spark.sparkContext.union([task_rdd_18])
    task_rdd_19 = simulate_task(union_rdd_19, '19', 87)

    union_rdd_25 = spark.sparkContext.union([task_rdd_24])
    task_rdd_25 = simulate_task(union_rdd_25, '25', 9)

    union_rdd_55 = spark.sparkContext.union([task_rdd_21, task_rdd_54])
    task_rdd_55 = simulate_task(union_rdd_55, '55', 86)

    union_rdd_33 = spark.sparkContext.union([task_rdd_29])
    task_rdd_33 = simulate_task(union_rdd_33, '33', 103)

    union_rdd_16 = spark.sparkContext.union([task_rdd_14])
    task_rdd_16 = simulate_task(union_rdd_16, '16', 2)

    union_rdd_20 = spark.sparkContext.union([task_rdd_1, task_rdd_19])
    task_rdd_20 = simulate_task(union_rdd_20, '20', 71)

    union_rdd_27 = spark.sparkContext.union([task_rdd_24, task_rdd_25])
    task_rdd_27 = simulate_task(union_rdd_27, '27', 36)

    union_rdd_42 = spark.sparkContext.union([task_rdd_55])
    task_rdd_42 = simulate_task(union_rdd_42, '42', 11)

    union_rdd_34 = spark.sparkContext.union([task_rdd_29, task_rdd_33])
    task_rdd_34 = simulate_task(union_rdd_34, '34', 10)

    union_rdd_43 = spark.sparkContext.union([task_rdd_42, task_rdd_63])
    task_rdd_43 = simulate_task(union_rdd_43, '43', 117)

    union_rdd_35 = spark.sparkContext.union([task_rdd_34])
    task_rdd_35 = simulate_task(union_rdd_35, '35', 50)

    union_rdd_30 = spark.sparkContext.union([task_rdd_35])
    task_rdd_30 = simulate_task(union_rdd_30, '30', 12)

    union_rdd_31 = spark.sparkContext.union([task_rdd_30])
    task_rdd_31 = simulate_task(union_rdd_31, '31', 283)

    union_rdd_32 = spark.sparkContext.union([task_rdd_29, task_rdd_31])
    task_rdd_32 = simulate_task(union_rdd_32, '32', 397)

    union_rdd_12 = spark.sparkContext.union([task_rdd_32])
    task_rdd_12 = simulate_task(union_rdd_12, '12', 346)

    union_rdd_44 = spark.sparkContext.union([task_rdd_12, task_rdd_13, task_rdd_43])
    task_rdd_44 = simulate_task(union_rdd_44, '44', 211)

    union_rdd_41 = spark.sparkContext.union([task_rdd_44])
    task_rdd_41 = simulate_task(union_rdd_41, '41', 9)

    union_rdd_60 = spark.sparkContext.union([task_rdd_41])
    task_rdd_60 = simulate_task(union_rdd_60, '60', 3)

    combined_rdd = spark.sparkContext.union([task_rdd_22, task_rdd_23])
    task_rdd = simulate_task(combined_rdd, 'J13_22_23', 20)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
