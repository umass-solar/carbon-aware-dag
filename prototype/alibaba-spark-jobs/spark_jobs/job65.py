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

    task_rdd_11 = simulate_task(base_rdd, '11', 2)
    task_rdd_16 = simulate_task(base_rdd, '16', 6)
    task_rdd_60 = simulate_task(base_rdd, '60', 5)
    task_rdd_5 = simulate_task(base_rdd, '5', 2)
    task_rdd_4 = simulate_task(base_rdd, '4', 2)
    task_rdd_1 = simulate_task(base_rdd, '1', 5)
    task_rdd_39 = simulate_task(base_rdd, '39', 1)
    task_rdd_17 = simulate_task(base_rdd, '17', 508)
    task_rdd_6 = simulate_task(base_rdd, '6', 43)
    task_rdd_24 = simulate_task(base_rdd, '24', 48)
    task_rdd_22 = simulate_task(base_rdd, '22', 1)
    task_rdd_20 = simulate_task(base_rdd, '20', 5)
    task_rdd_19 = simulate_task(base_rdd, '19', 16)
    task_rdd_132 = simulate_task(base_rdd, '132', 4)
    task_rdd_7 = simulate_task(base_rdd, '7', 6)
    task_rdd_3 = simulate_task(base_rdd, '3', 3)
    task_rdd_2 = simulate_task(base_rdd, '2', 26)
    task_rdd_21 = simulate_task(base_rdd, '21', 1)
    task_rdd_25 = simulate_task(base_rdd, '25', 4)
    task_rdd_14 = simulate_task(base_rdd, '14', 4)
    task_rdd_28 = simulate_task(base_rdd, '28', 7)
    task_rdd_27 = simulate_task(base_rdd, '27', 4)
    task_rdd_26 = simulate_task(base_rdd, '26', 1)
    task_rdd_35 = simulate_task(base_rdd, '35', 193)
    task_rdd_31 = simulate_task(base_rdd, '31', 54)

    union_rdd_36 = spark.sparkContext.union([task_rdd_35])
    task_rdd_36 = simulate_task(union_rdd_36, '36', 2)

    union_rdd_12 = spark.sparkContext.union([task_rdd_1, task_rdd_2, task_rdd_5, task_rdd_11])
    task_rdd_12 = simulate_task(union_rdd_12, '12', 12)

    union_rdd_10 = spark.sparkContext.union([task_rdd_3, task_rdd_4])
    task_rdd_10 = simulate_task(union_rdd_10, '10', 36)

    union_rdd_40 = spark.sparkContext.union([task_rdd_17, task_rdd_24, task_rdd_39])
    task_rdd_40 = simulate_task(union_rdd_40, '40', 15)

    union_rdd_23 = spark.sparkContext.union([task_rdd_7, task_rdd_20, task_rdd_22])
    task_rdd_23 = simulate_task(union_rdd_23, '23', 251)

    union_rdd_51 = spark.sparkContext.union([task_rdd_132])
    task_rdd_51 = simulate_task(union_rdd_51, '51', 4)

    union_rdd_15 = spark.sparkContext.union([task_rdd_25])
    task_rdd_15 = simulate_task(union_rdd_15, '15', 100)

    union_rdd_29 = spark.sparkContext.union([task_rdd_26])
    task_rdd_29 = simulate_task(union_rdd_29, '29', 25)

    union_rdd_37 = spark.sparkContext.union([task_rdd_7, task_rdd_36])
    task_rdd_37 = simulate_task(union_rdd_37, '37', 180)

    union_rdd_8 = spark.sparkContext.union([task_rdd_7, task_rdd_10])
    task_rdd_8 = simulate_task(union_rdd_8, '8', 341)

    union_rdd_18 = spark.sparkContext.union([task_rdd_39, task_rdd_40])
    task_rdd_18 = simulate_task(union_rdd_18, '18', 16)

    union_rdd_30 = spark.sparkContext.union([task_rdd_27, task_rdd_29])
    task_rdd_30 = simulate_task(union_rdd_30, '30', 111)

    union_rdd_38 = spark.sparkContext.union([task_rdd_16, task_rdd_29, task_rdd_36, task_rdd_37])
    task_rdd_38 = simulate_task(union_rdd_38, '38', 421)

    union_rdd_9 = spark.sparkContext.union([task_rdd_2, task_rdd_8])
    task_rdd_9 = simulate_task(union_rdd_9, '9', 2)

    union_rdd_13 = spark.sparkContext.union([task_rdd_6, task_rdd_7, task_rdd_18])
    task_rdd_13 = simulate_task(union_rdd_13, '13', 139)

    union_rdd_33 = spark.sparkContext.union([task_rdd_11, task_rdd_19])
    task_rdd_33 = simulate_task(union_rdd_33, '33', 4)

    union_rdd_34 = spark.sparkContext.union([task_rdd_5, task_rdd_6, task_rdd_7, task_rdd_33])
    task_rdd_34 = simulate_task(union_rdd_34, '34', 167)

    combined_rdd = spark.sparkContext.union([task_rdd_31, task_rdd_34])
    task_rdd = simulate_task(combined_rdd, 'J32_31_34', 103)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
