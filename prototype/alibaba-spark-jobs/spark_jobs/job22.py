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

    task_rdd_7 = simulate_task(base_rdd, '7', 1)
    task_rdd_9 = simulate_task(base_rdd, '9', 26)
    task_rdd_15 = simulate_task(base_rdd, '15', 76)
    task_rdd_10 = simulate_task(base_rdd, '10', 195)
    task_rdd_11 = simulate_task(base_rdd, '11', 5)
    task_rdd_6 = simulate_task(base_rdd, '6', 11)
    task_rdd_18 = simulate_task(base_rdd, '18', 117)
    task_rdd_13 = simulate_task(base_rdd, '13', 69)
    task_rdd_12 = simulate_task(base_rdd, '12', 4)
    task_rdd_3 = simulate_task(base_rdd, '3', 5)
    task_rdd_53 = simulate_task(base_rdd, '53', 6)
    task_rdd_4 = simulate_task(base_rdd, '4', 16)
    task_rdd_5 = simulate_task(base_rdd, '5', 209)
    task_rdd_2 = simulate_task(base_rdd, '2', 6)
    task_rdd_1 = simulate_task(base_rdd, '1', 12)
    task_rdd_131 = simulate_task(base_rdd, '131', 4)
    task_rdd_30 = simulate_task(base_rdd, '30', 42)
    task_rdd_55 = simulate_task(base_rdd, '55', 4)

    union_rdd_8 = spark.sparkContext.union([task_rdd_2, task_rdd_3, task_rdd_5, task_rdd_7, task_rdd_9])
    task_rdd_8 = simulate_task(union_rdd_8, '8', 42)

    union_rdd_17 = spark.sparkContext.union([task_rdd_5, task_rdd_6, task_rdd_7, task_rdd_10])
    task_rdd_17 = simulate_task(union_rdd_17, '17', 74)

    union_rdd_19 = spark.sparkContext.union([task_rdd_2, task_rdd_4, task_rdd_18])
    task_rdd_19 = simulate_task(union_rdd_19, '19', 68)

    union_rdd_132 = spark.sparkContext.union([task_rdd_131])
    task_rdd_132 = simulate_task(union_rdd_132, '132', 1)

    union_rdd_14 = spark.sparkContext.union([task_rdd_4, task_rdd_6, task_rdd_7, task_rdd_8])
    task_rdd_14 = simulate_task(union_rdd_14, '14', 131)

    union_rdd_20 = spark.sparkContext.union([task_rdd_13, task_rdd_19])
    task_rdd_20 = simulate_task(union_rdd_20, '20', 2)

    union_rdd_16 = spark.sparkContext.union([task_rdd_7, task_rdd_9, task_rdd_13, task_rdd_14])
    task_rdd_16 = simulate_task(union_rdd_16, '16', 58)

    union_rdd_21 = spark.sparkContext.union([task_rdd_3, task_rdd_20])
    task_rdd_21 = simulate_task(union_rdd_21, '21', 3)

    union_rdd_22 = spark.sparkContext.union([task_rdd_3, task_rdd_5, task_rdd_18, task_rdd_21])
    task_rdd_22 = simulate_task(union_rdd_22, '22', 19)

    union_rdd_31 = spark.sparkContext.union([task_rdd_22])
    task_rdd_31 = simulate_task(union_rdd_31, '31', 216)

    union_rdd_32 = spark.sparkContext.union([task_rdd_1, task_rdd_31])
    task_rdd_32 = simulate_task(union_rdd_32, '32', 29)

    combined_rdd = spark.sparkContext.union([task_rdd_4, task_rdd_5, task_rdd_6, task_rdd_15, task_rdd_16])
    task_rdd = simulate_task(combined_rdd, 'J7_4_5_6_15_16', 30)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
