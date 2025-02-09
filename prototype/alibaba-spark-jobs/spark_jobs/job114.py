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

    task_rdd_16 = simulate_task(base_rdd, '16', 60)
    task_rdd_18 = simulate_task(base_rdd, '18', 14)
    task_rdd_3 = simulate_task(base_rdd, '3', 21)
    task_rdd_6 = simulate_task(base_rdd, '6', 7)
    task_rdd_5 = simulate_task(base_rdd, '5', 1)
    task_rdd_8 = simulate_task(base_rdd, '8', 13)
    task_rdd_2 = simulate_task(base_rdd, '2', 9)
    task_rdd_12 = simulate_task(base_rdd, '12', 37)
    task_rdd_34 = simulate_task(base_rdd, '34', 26)
    task_rdd_132 = simulate_task(base_rdd, '132', 4)
    task_rdd_11 = simulate_task(base_rdd, '11', 240)
    task_rdd_10 = simulate_task(base_rdd, '10', 23)
    task_rdd_9 = simulate_task(base_rdd, '9', 87)
    task_rdd_47 = simulate_task(base_rdd, '47', 1)
    task_rdd_100 = simulate_task(base_rdd, '100', 1)
    task_rdd_23 = simulate_task(base_rdd, '23', 22)
    task_rdd_43 = simulate_task(base_rdd, '43', 47)
    task_rdd_21 = simulate_task(base_rdd, '21', 32)

    union_rdd_24 = spark.sparkContext.union([task_rdd_18])
    task_rdd_24 = simulate_task(union_rdd_24, '24', 380)

    union_rdd_1 = spark.sparkContext.union([task_rdd_21])
    task_rdd_1 = simulate_task(union_rdd_1, '1', 86)

    union_rdd_4 = spark.sparkContext.union([task_rdd_43])
    task_rdd_4 = simulate_task(union_rdd_4, '4', 3)

    union_rdd_51 = spark.sparkContext.union([task_rdd_10])
    task_rdd_51 = simulate_task(union_rdd_51, '51', 4)

    union_rdd_120 = spark.sparkContext.union([task_rdd_132])
    task_rdd_120 = simulate_task(union_rdd_120, '120', 4)

    union_rdd_39 = spark.sparkContext.union([task_rdd_100])
    task_rdd_39 = simulate_task(union_rdd_39, '39', 15)

    union_rdd_20 = spark.sparkContext.union([task_rdd_23])
    task_rdd_20 = simulate_task(union_rdd_20, '20', 125)

    union_rdd_25 = spark.sparkContext.union([task_rdd_1, task_rdd_3, task_rdd_24])
    task_rdd_25 = simulate_task(union_rdd_25, '25', 35)

    union_rdd_17 = spark.sparkContext.union([task_rdd_2, task_rdd_4, task_rdd_5, task_rdd_8, task_rdd_11, task_rdd_12, task_rdd_16])
    task_rdd_17 = simulate_task(union_rdd_17, '17', 674)

    union_rdd_7 = spark.sparkContext.union([task_rdd_1, task_rdd_2, task_rdd_6])
    task_rdd_7 = simulate_task(union_rdd_7, '7', 413)

    union_rdd_19 = spark.sparkContext.union([task_rdd_4])
    task_rdd_19 = simulate_task(union_rdd_19, '19', 11)

    union_rdd_52 = spark.sparkContext.union([task_rdd_11, task_rdd_51])
    task_rdd_52 = simulate_task(union_rdd_52, '52', 191)

    union_rdd_32 = spark.sparkContext.union([task_rdd_39])
    task_rdd_32 = simulate_task(union_rdd_32, '32', 50)

    union_rdd_22 = spark.sparkContext.union([task_rdd_20])
    task_rdd_22 = simulate_task(union_rdd_22, '22', 91)

    union_rdd_26 = spark.sparkContext.union([task_rdd_6, task_rdd_16, task_rdd_25])
    task_rdd_26 = simulate_task(union_rdd_26, '26', 104)

    union_rdd_29 = spark.sparkContext.union([task_rdd_26])
    task_rdd_29 = simulate_task(union_rdd_29, '29', 64)

    union_rdd_30 = spark.sparkContext.union([task_rdd_1, task_rdd_3, task_rdd_4, task_rdd_6, task_rdd_9, task_rdd_11, task_rdd_12, task_rdd_29])
    task_rdd_30 = simulate_task(union_rdd_30, '30', 9)

    union_rdd_13 = spark.sparkContext.union([task_rdd_30])
    task_rdd_13 = simulate_task(union_rdd_13, '13', 5)

    union_rdd_14 = spark.sparkContext.union([task_rdd_2, task_rdd_6, task_rdd_8, task_rdd_13])
    task_rdd_14 = simulate_task(union_rdd_14, '14', 94)

    union_rdd_15 = spark.sparkContext.union([task_rdd_9, task_rdd_14])
    task_rdd_15 = simulate_task(union_rdd_15, '15', 18)

    combined_rdd = spark.sparkContext.union([task_rdd_8, task_rdd_10, task_rdd_15])
    task_rdd = simulate_task(combined_rdd, 'J16_8_10_15', 11)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
