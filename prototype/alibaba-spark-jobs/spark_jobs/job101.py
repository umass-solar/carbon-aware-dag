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

    task_rdd_11 = simulate_task(base_rdd, '11', 38)
    task_rdd_10 = simulate_task(base_rdd, '10', 15)
    task_rdd_21 = simulate_task(base_rdd, '21', 2)
    task_rdd_18 = simulate_task(base_rdd, '18', 3)
    task_rdd_17 = simulate_task(base_rdd, '17', 23)
    task_rdd_9 = simulate_task(base_rdd, '9', 4)
    task_rdd_8 = simulate_task(base_rdd, '8', 16)
    task_rdd_7 = simulate_task(base_rdd, '7', 15)
    task_rdd_6 = simulate_task(base_rdd, '6', 15)
    task_rdd_4 = simulate_task(base_rdd, '4', 14)
    task_rdd_3 = simulate_task(base_rdd, '3', 2)
    task_rdd_1 = simulate_task(base_rdd, '1', 1)
    task_rdd_2 = simulate_task(base_rdd, '2', 145)
    task_rdd_5 = simulate_task(base_rdd, '5', 16)
    task_rdd_47 = simulate_task(base_rdd, '47', 2)
    task_rdd_38 = simulate_task(base_rdd, '38', 75)
    task_rdd_26 = simulate_task(base_rdd, '26', 42)
    task_rdd_24 = simulate_task(base_rdd, '24', 42)
    task_rdd_29 = simulate_task(base_rdd, '29', 15)

    union_rdd_13 = spark.sparkContext.union([task_rdd_5, task_rdd_11])
    task_rdd_13 = simulate_task(union_rdd_13, '13', 56)

    union_rdd_37 = spark.sparkContext.union([task_rdd_1, task_rdd_2])
    task_rdd_37 = simulate_task(union_rdd_37, '37', 86)

    union_rdd_20 = spark.sparkContext.union([task_rdd_5, task_rdd_17])
    task_rdd_20 = simulate_task(union_rdd_20, '20', 183)

    union_rdd_15 = spark.sparkContext.union([task_rdd_1, task_rdd_3, task_rdd_5, task_rdd_6])
    task_rdd_15 = simulate_task(union_rdd_15, '15', 345)

    union_rdd_28 = spark.sparkContext.union([task_rdd_26])
    task_rdd_28 = simulate_task(union_rdd_28, '28', 70)

    union_rdd_31 = spark.sparkContext.union([])
    task_rdd_31 = simulate_task(union_rdd_31, '31', 1151)

    union_rdd_25 = spark.sparkContext.union([task_rdd_24, task_rdd_37])
    task_rdd_25 = simulate_task(union_rdd_25, '25', 9)

    union_rdd_32 = spark.sparkContext.union([task_rdd_31])
    task_rdd_32 = simulate_task(union_rdd_32, '32', 137)

    union_rdd_22 = spark.sparkContext.union([task_rdd_21, task_rdd_25])
    task_rdd_22 = simulate_task(union_rdd_22, '22', 10)

    union_rdd_23 = spark.sparkContext.union([task_rdd_6, task_rdd_8, task_rdd_9, task_rdd_22])
    task_rdd_23 = simulate_task(union_rdd_23, '23', 172)

    union_rdd_16 = spark.sparkContext.union([task_rdd_18, task_rdd_23])
    task_rdd_16 = simulate_task(union_rdd_16, '16', 30)

    combined_rdd = spark.sparkContext.union([task_rdd_7, task_rdd_9, task_rdd_13])
    task_rdd = simulate_task(combined_rdd, 'J14_7_9_13', 404)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
