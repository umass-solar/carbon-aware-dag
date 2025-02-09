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

    task_rdd_12 = simulate_task(base_rdd, '12', 10)
    task_rdd_8 = simulate_task(base_rdd, '8', 100)
    task_rdd_4 = simulate_task(base_rdd, '4', 262)
    task_rdd_50 = simulate_task(base_rdd, '50', 31)
    task_rdd_7 = simulate_task(base_rdd, '7', 3)
    task_rdd_5 = simulate_task(base_rdd, '5', 138)
    task_rdd_2 = simulate_task(base_rdd, '2', 6)
    task_rdd_1 = simulate_task(base_rdd, '1', 5)
    task_rdd_3 = simulate_task(base_rdd, '3', 211)
    task_rdd_6 = simulate_task(base_rdd, '6', 1)
    task_rdd_55 = simulate_task(base_rdd, '55', 5)
    task_rdd_83 = simulate_task(base_rdd, '83', 3)
    task_rdd_56 = simulate_task(base_rdd, '56', 116)
    task_rdd_20 = simulate_task(base_rdd, '20', 10)

    union_rdd_9 = spark.sparkContext.union([task_rdd_1, task_rdd_5, task_rdd_8])
    task_rdd_9 = simulate_task(union_rdd_9, '9', 138)

    union_rdd_62 = spark.sparkContext.union([task_rdd_50])
    task_rdd_62 = simulate_task(union_rdd_62, '62', 6)

    union_rdd_13 = spark.sparkContext.union([task_rdd_56])
    task_rdd_13 = simulate_task(union_rdd_13, '13', 10)

    union_rdd_15 = spark.sparkContext.union([task_rdd_5])
    task_rdd_15 = simulate_task(union_rdd_15, '15', 466)

    union_rdd_67 = spark.sparkContext.union([task_rdd_55])
    task_rdd_67 = simulate_task(union_rdd_67, '67', 260)

    union_rdd_80 = spark.sparkContext.union([task_rdd_83])
    task_rdd_80 = simulate_task(union_rdd_80, '80', 7)

    union_rdd_10 = spark.sparkContext.union([task_rdd_1, task_rdd_2, task_rdd_3, task_rdd_8, task_rdd_9])
    task_rdd_10 = simulate_task(union_rdd_10, '10', 12)

    union_rdd_57 = spark.sparkContext.union([task_rdd_62])
    task_rdd_57 = simulate_task(union_rdd_57, '57', 120)

    union_rdd_16 = spark.sparkContext.union([task_rdd_1, task_rdd_2, task_rdd_15])
    task_rdd_16 = simulate_task(union_rdd_16, '16', 10)

    union_rdd_61 = spark.sparkContext.union([task_rdd_80])
    task_rdd_61 = simulate_task(union_rdd_61, '61', 2)

    union_rdd_11 = spark.sparkContext.union([task_rdd_1, task_rdd_2, task_rdd_4, task_rdd_8, task_rdd_10])
    task_rdd_11 = simulate_task(union_rdd_11, '11', 23)

    union_rdd_14 = spark.sparkContext.union([task_rdd_13, task_rdd_57])
    task_rdd_14 = simulate_task(union_rdd_14, '14', 21)

    union_rdd_17 = spark.sparkContext.union([task_rdd_16])
    task_rdd_17 = simulate_task(union_rdd_17, '17', 7)

    union_rdd_21 = spark.sparkContext.union([task_rdd_61])
    task_rdd_21 = simulate_task(union_rdd_21, '21', 5)

    union_rdd_22 = spark.sparkContext.union([task_rdd_21])
    task_rdd_22 = simulate_task(union_rdd_22, '22', 7)

    combined_rdd = spark.sparkContext.union([task_rdd_3, task_rdd_7, task_rdd_11])
    task_rdd = simulate_task(combined_rdd, 'R12_3_7_11', 70)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
