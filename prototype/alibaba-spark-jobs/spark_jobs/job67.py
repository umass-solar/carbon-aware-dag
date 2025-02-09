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

    task_rdd_50 = simulate_task(base_rdd, '50', 2)
    task_rdd_14 = simulate_task(base_rdd, '14', 4)
    task_rdd_8 = simulate_task(base_rdd, '8', 42)
    task_rdd_7 = simulate_task(base_rdd, '7', 2)
    task_rdd_2 = simulate_task(base_rdd, '2', 48)
    task_rdd_26 = simulate_task(base_rdd, '26', 1)
    task_rdd_22 = simulate_task(base_rdd, '22', 35)
    task_rdd_10 = simulate_task(base_rdd, '10', 8)
    task_rdd_4 = simulate_task(base_rdd, '4', 180)
    task_rdd_3 = simulate_task(base_rdd, '3', 2)
    task_rdd_1 = simulate_task(base_rdd, '1', 12)
    task_rdd_6 = simulate_task(base_rdd, '6', 4)
    task_rdd_45 = simulate_task(base_rdd, '45', 7)
    task_rdd_41 = simulate_task(base_rdd, '41', 511)
    task_rdd_24 = simulate_task(base_rdd, '24', 7)
    task_rdd_13 = simulate_task(base_rdd, '13', 1)
    task_rdd_35 = simulate_task(base_rdd, '35', 2)

    union_rdd_51 = spark.sparkContext.union([task_rdd_50])
    task_rdd_51 = simulate_task(union_rdd_51, '51', 25)

    union_rdd_15 = spark.sparkContext.union([task_rdd_7, task_rdd_10, task_rdd_13, task_rdd_14])
    task_rdd_15 = simulate_task(union_rdd_15, '15', 4)

    union_rdd_17 = spark.sparkContext.union([task_rdd_2, task_rdd_4, task_rdd_6, task_rdd_8])
    task_rdd_17 = simulate_task(union_rdd_17, '17', 27)

    union_rdd_27 = spark.sparkContext.union([task_rdd_26])
    task_rdd_27 = simulate_task(union_rdd_27, '27', 17)

    union_rdd_23 = spark.sparkContext.union([task_rdd_1, task_rdd_4, task_rdd_10, task_rdd_22])
    task_rdd_23 = simulate_task(union_rdd_23, '23', 14)

    union_rdd_44 = spark.sparkContext.union([task_rdd_41])
    task_rdd_44 = simulate_task(union_rdd_44, '44', 4)

    union_rdd_25 = spark.sparkContext.union([task_rdd_24])
    task_rdd_25 = simulate_task(union_rdd_25, '25', 9)

    union_rdd_36 = spark.sparkContext.union([task_rdd_35])
    task_rdd_36 = simulate_task(union_rdd_36, '36', 317)

    union_rdd_9 = spark.sparkContext.union([task_rdd_36])
    task_rdd_9 = simulate_task(union_rdd_9, '9', 347)

    union_rdd_16 = spark.sparkContext.union([task_rdd_15, task_rdd_51])
    task_rdd_16 = simulate_task(union_rdd_16, '16', 8)

    union_rdd_20 = spark.sparkContext.union([task_rdd_1, task_rdd_2, task_rdd_3, task_rdd_4, task_rdd_6, task_rdd_8, task_rdd_17])
    task_rdd_20 = simulate_task(union_rdd_20, '20', 146)

    union_rdd_28 = spark.sparkContext.union([task_rdd_27])
    task_rdd_28 = simulate_task(union_rdd_28, '28', 8)

    union_rdd_42 = spark.sparkContext.union([task_rdd_25])
    task_rdd_42 = simulate_task(union_rdd_42, '42', 183)

    union_rdd_5 = spark.sparkContext.union([task_rdd_9])
    task_rdd_5 = simulate_task(union_rdd_5, '5', 2)

    union_rdd_31 = spark.sparkContext.union([])
    task_rdd_31 = simulate_task(union_rdd_31, '31', 5)

    union_rdd_18 = spark.sparkContext.union([task_rdd_1])
    task_rdd_18 = simulate_task(union_rdd_18, '18', 24)

    union_rdd_47 = spark.sparkContext.union([])
    task_rdd_47 = simulate_task(union_rdd_47, '47', 8)

    union_rdd_95 = spark.sparkContext.union([task_rdd_5])
    task_rdd_95 = simulate_task(union_rdd_95, '95', 4)

    union_rdd_48 = spark.sparkContext.union([task_rdd_13, task_rdd_47])
    task_rdd_48 = simulate_task(union_rdd_48, '48', 494)

    union_rdd_116 = spark.sparkContext.union([task_rdd_95])
    task_rdd_116 = simulate_task(union_rdd_116, '116', 2)

    union_rdd_120 = spark.sparkContext.union([task_rdd_116])
    task_rdd_120 = simulate_task(union_rdd_120, '120', 72)

    union_rdd_49 = spark.sparkContext.union([task_rdd_120])
    task_rdd_49 = simulate_task(union_rdd_49, '49', 4)

    union_rdd_52 = spark.sparkContext.union([task_rdd_49])
    task_rdd_52 = simulate_task(union_rdd_52, '52', 3)

    union_rdd_19 = spark.sparkContext.union([task_rdd_52])
    task_rdd_19 = simulate_task(union_rdd_19, '19', 5)

    union_rdd_11 = spark.sparkContext.union([task_rdd_9, task_rdd_10, task_rdd_19])
    task_rdd_11 = simulate_task(union_rdd_11, '11', 45)

    combined_rdd = spark.sparkContext.union([task_rdd_4, task_rdd_8, task_rdd_10, task_rdd_11])
    task_rdd = simulate_task(combined_rdd, 'J12_4_8_10_11', 3)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
