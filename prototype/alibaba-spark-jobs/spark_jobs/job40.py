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

    task_rdd_18 = simulate_task(base_rdd, '18', 4)
    task_rdd_13 = simulate_task(base_rdd, '13', 33)
    task_rdd_20 = simulate_task(base_rdd, '20', 2)
    task_rdd_19 = simulate_task(base_rdd, '19', 311)
    task_rdd_12 = simulate_task(base_rdd, '12', 60)
    task_rdd_11 = simulate_task(base_rdd, '11', 45)
    task_rdd_10 = simulate_task(base_rdd, '10', 98)
    task_rdd_9 = simulate_task(base_rdd, '9', 135)
    task_rdd_7 = simulate_task(base_rdd, '7', 7)
    task_rdd_6 = simulate_task(base_rdd, '6', 1)
    task_rdd_35 = simulate_task(base_rdd, '35', 5)
    task_rdd_4 = simulate_task(base_rdd, '4', 6)
    task_rdd_3 = simulate_task(base_rdd, '3', 6)
    task_rdd_1 = simulate_task(base_rdd, '1', 4)
    task_rdd_8 = simulate_task(base_rdd, '8', 9)
    task_rdd_52 = simulate_task(base_rdd, '52', 17)
    task_rdd_24 = simulate_task(base_rdd, '24', 16)
    task_rdd_41 = simulate_task(base_rdd, '41', 146)

    union_rdd_21 = spark.sparkContext.union([task_rdd_19, task_rdd_20])
    task_rdd_21 = simulate_task(union_rdd_21, '21', 254)

    union_rdd_5 = spark.sparkContext.union([task_rdd_41])
    task_rdd_5 = simulate_task(union_rdd_5, '5', 58)

    union_rdd_36 = spark.sparkContext.union([task_rdd_35])
    task_rdd_36 = simulate_task(union_rdd_36, '36', 28)

    union_rdd_54 = spark.sparkContext.union([task_rdd_52])
    task_rdd_54 = simulate_task(union_rdd_54, '54', 10)

    union_rdd_14 = spark.sparkContext.union([task_rdd_4, task_rdd_5, task_rdd_6, task_rdd_10, task_rdd_11, task_rdd_12, task_rdd_13])
    task_rdd_14 = simulate_task(union_rdd_14, '14', 148)

    union_rdd_22 = spark.sparkContext.union([task_rdd_18, task_rdd_19, task_rdd_20, task_rdd_21])
    task_rdd_22 = simulate_task(union_rdd_22, '22', 5)

    union_rdd_26 = spark.sparkContext.union([task_rdd_36])
    task_rdd_26 = simulate_task(union_rdd_26, '26', 195)

    union_rdd_55 = spark.sparkContext.union([task_rdd_24, task_rdd_54])
    task_rdd_55 = simulate_task(union_rdd_55, '55', 66)

    union_rdd_15 = spark.sparkContext.union([task_rdd_4, task_rdd_10, task_rdd_14])
    task_rdd_15 = simulate_task(union_rdd_15, '15', 1653)

    union_rdd_23 = spark.sparkContext.union([task_rdd_1, task_rdd_3, task_rdd_9, task_rdd_22])
    task_rdd_23 = simulate_task(union_rdd_23, '23', 489)

    union_rdd_2 = spark.sparkContext.union([task_rdd_26])
    task_rdd_2 = simulate_task(union_rdd_2, '2', 5)

    union_rdd_16 = spark.sparkContext.union([task_rdd_4, task_rdd_15])
    task_rdd_16 = simulate_task(union_rdd_16, '16', 8)

    union_rdd_17 = spark.sparkContext.union([task_rdd_13, task_rdd_16])
    task_rdd_17 = simulate_task(union_rdd_17, '17', 6)

    combined_rdd = spark.sparkContext.union([task_rdd_3, task_rdd_4, task_rdd_5, task_rdd_6, task_rdd_7, task_rdd_8, task_rdd_17])
    task_rdd = simulate_task(combined_rdd, 'J18_3_4_5_6_7_8_17', 4)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
