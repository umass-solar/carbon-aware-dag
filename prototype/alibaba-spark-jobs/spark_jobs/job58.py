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

    task_rdd_30 = simulate_task(base_rdd, '30', 2)
    task_rdd_18 = simulate_task(base_rdd, '18', 9)
    task_rdd_17 = simulate_task(base_rdd, '17', 83)
    task_rdd_16 = simulate_task(base_rdd, '16', 3)
    task_rdd_11 = simulate_task(base_rdd, '11', 24)
    task_rdd_10 = simulate_task(base_rdd, '10', 116)
    task_rdd_13 = simulate_task(base_rdd, '13', 30)
    task_rdd_3 = simulate_task(base_rdd, '3', 15)
    task_rdd_9 = simulate_task(base_rdd, '9', 1)
    task_rdd_5 = simulate_task(base_rdd, '5', 1)
    task_rdd_6 = simulate_task(base_rdd, '6', 90)
    task_rdd_59 = simulate_task(base_rdd, '59', 17)
    task_rdd_2 = simulate_task(base_rdd, '2', 14)
    task_rdd_8 = simulate_task(base_rdd, '8', 11)
    task_rdd_14 = simulate_task(base_rdd, '14', 19)
    task_rdd_7 = simulate_task(base_rdd, '7', 74)
    task_rdd_15 = simulate_task(base_rdd, '15', 11)
    task_rdd_4 = simulate_task(base_rdd, '4', 44)
    task_rdd_37 = simulate_task(base_rdd, '37', 2)
    task_rdd_32 = simulate_task(base_rdd, '32', 31)
    task_rdd_28 = simulate_task(base_rdd, '28', 102)
    task_rdd_24 = simulate_task(base_rdd, '24', 71)
    task_rdd_21 = simulate_task(base_rdd, '21', 4)
    task_rdd_132 = simulate_task(base_rdd, '132', 4)

    union_rdd_31 = spark.sparkContext.union([task_rdd_30])
    task_rdd_31 = simulate_task(union_rdd_31, '31', 166)

    union_rdd_25 = spark.sparkContext.union([task_rdd_9, task_rdd_11])
    task_rdd_25 = simulate_task(union_rdd_25, '25', 54)

    union_rdd_12 = spark.sparkContext.union([task_rdd_2, task_rdd_3])
    task_rdd_12 = simulate_task(union_rdd_12, '12', 42)

    union_rdd_60 = spark.sparkContext.union([task_rdd_59])
    task_rdd_60 = simulate_task(union_rdd_60, '60', 18)

    union_rdd_38 = spark.sparkContext.union([task_rdd_32, task_rdd_37])
    task_rdd_38 = simulate_task(union_rdd_38, '38', 44)

    union_rdd_29 = spark.sparkContext.union([task_rdd_24, task_rdd_28])
    task_rdd_29 = simulate_task(union_rdd_29, '29', 1)

    union_rdd_100 = spark.sparkContext.union([task_rdd_132])
    task_rdd_100 = simulate_task(union_rdd_100, '100', 4)

    union_rdd_19 = spark.sparkContext.union([task_rdd_18, task_rdd_31])
    task_rdd_19 = simulate_task(union_rdd_19, '19', 25)

    union_rdd_26 = spark.sparkContext.union([task_rdd_2, task_rdd_3, task_rdd_7, task_rdd_13, task_rdd_14, task_rdd_24, task_rdd_25])
    task_rdd_26 = simulate_task(union_rdd_26, '26', 41)

    union_rdd_43 = spark.sparkContext.union([task_rdd_38])
    task_rdd_43 = simulate_task(union_rdd_43, '43', 7)

    union_rdd_33 = spark.sparkContext.union([task_rdd_29])
    task_rdd_33 = simulate_task(union_rdd_33, '33', 103)

    union_rdd_47 = spark.sparkContext.union([task_rdd_100])
    task_rdd_47 = simulate_task(union_rdd_47, '47', 15)

    union_rdd_20 = spark.sparkContext.union([task_rdd_11, task_rdd_13, task_rdd_14, task_rdd_19])
    task_rdd_20 = simulate_task(union_rdd_20, '20', 32)

    union_rdd_1 = spark.sparkContext.union([task_rdd_47])
    task_rdd_1 = simulate_task(union_rdd_1, '1', 2)

    union_rdd_27 = spark.sparkContext.union([task_rdd_26])
    task_rdd_27 = simulate_task(union_rdd_27, '27', 270)

    union_rdd_44 = spark.sparkContext.union([task_rdd_43])
    task_rdd_44 = simulate_task(union_rdd_44, '44', 8)

    union_rdd_34 = spark.sparkContext.union([task_rdd_33])
    task_rdd_34 = simulate_task(union_rdd_34, '34', 469)

    union_rdd_22 = spark.sparkContext.union([task_rdd_21])
    task_rdd_22 = simulate_task(union_rdd_22, '22', 12)

    union_rdd_36 = spark.sparkContext.union([])
    task_rdd_36 = simulate_task(union_rdd_36, '36', 89)

    combined_rdd = spark.sparkContext.union([task_rdd_3, task_rdd_4, task_rdd_5, task_rdd_6, task_rdd_7, task_rdd_22])
    task_rdd = simulate_task(combined_rdd, 'J23_3_4_5_6_7_22', 30)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
