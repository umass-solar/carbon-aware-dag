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

    task_rdd_12 = simulate_task(base_rdd, '12', 1)
    task_rdd_11 = simulate_task(base_rdd, '11', 373)
    task_rdd_10 = simulate_task(base_rdd, '10', 60)
    task_rdd_14 = simulate_task(base_rdd, '14', 2)
    task_rdd_8 = simulate_task(base_rdd, '8', 10)
    task_rdd_7 = simulate_task(base_rdd, '7', 2)
    task_rdd_3 = simulate_task(base_rdd, '3', 4)
    task_rdd_24 = simulate_task(base_rdd, '24', 2)
    task_rdd_1 = simulate_task(base_rdd, '1', 28)
    task_rdd_2 = simulate_task(base_rdd, '2', 6)
    task_rdd_29 = simulate_task(base_rdd, '29', 2)
    task_rdd_5 = simulate_task(base_rdd, '5', 50)
    task_rdd_17 = simulate_task(base_rdd, '17', 1)
    task_rdd_35 = simulate_task(base_rdd, '35', 113)

    union_rdd_13 = spark.sparkContext.union([task_rdd_1, task_rdd_8, task_rdd_10, task_rdd_11, task_rdd_12])
    task_rdd_13 = simulate_task(union_rdd_13, '13', 362)

    union_rdd_15 = spark.sparkContext.union([task_rdd_1, task_rdd_2, task_rdd_3, task_rdd_7, task_rdd_14])
    task_rdd_15 = simulate_task(union_rdd_15, '15', 18)

    union_rdd_25 = spark.sparkContext.union([task_rdd_1, task_rdd_24])
    task_rdd_25 = simulate_task(union_rdd_25, '25', 6)

    union_rdd_16 = spark.sparkContext.union([task_rdd_2])
    task_rdd_16 = simulate_task(union_rdd_16, '16', 1997)

    union_rdd_30 = spark.sparkContext.union([task_rdd_29])
    task_rdd_30 = simulate_task(union_rdd_30, '30', 742)

    union_rdd_27 = spark.sparkContext.union([task_rdd_17])
    task_rdd_27 = simulate_task(union_rdd_27, '27', 22)

    union_rdd_18 = spark.sparkContext.union([task_rdd_12, task_rdd_13])
    task_rdd_18 = simulate_task(union_rdd_18, '18', 454)

    union_rdd_6 = spark.sparkContext.union([task_rdd_2, task_rdd_5, task_rdd_15])
    task_rdd_6 = simulate_task(union_rdd_6, '6', 7)

    union_rdd_23 = spark.sparkContext.union([task_rdd_5, task_rdd_16])
    task_rdd_23 = simulate_task(union_rdd_23, '23', 320)

    union_rdd_32 = spark.sparkContext.union([task_rdd_30])
    task_rdd_32 = simulate_task(union_rdd_32, '32', 6)

    union_rdd_28 = spark.sparkContext.union([task_rdd_27])
    task_rdd_28 = simulate_task(union_rdd_28, '28', 25)

    union_rdd_9 = spark.sparkContext.union([task_rdd_1, task_rdd_2, task_rdd_6])
    task_rdd_9 = simulate_task(union_rdd_9, '9', 319)

    union_rdd_26 = spark.sparkContext.union([task_rdd_23, task_rdd_25])
    task_rdd_26 = simulate_task(union_rdd_26, '26', 156)

    union_rdd_4 = spark.sparkContext.union([task_rdd_28])
    task_rdd_4 = simulate_task(union_rdd_4, '4', 168)

    union_rdd_20 = spark.sparkContext.union([task_rdd_2])
    task_rdd_20 = simulate_task(union_rdd_20, '20', 115)

    union_rdd_34 = spark.sparkContext.union([])
    task_rdd_34 = simulate_task(union_rdd_34, '34', 178)

    union_rdd_21 = spark.sparkContext.union([task_rdd_20])
    task_rdd_21 = simulate_task(union_rdd_21, '21', 23)

    union_rdd_22 = spark.sparkContext.union([task_rdd_5, task_rdd_21])
    task_rdd_22 = simulate_task(union_rdd_22, '22', 9)

    combined_rdd = spark.sparkContext.union([task_rdd_12, task_rdd_14, task_rdd_18])
    task_rdd = simulate_task(combined_rdd, 'J19_12_14_18', 4)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
