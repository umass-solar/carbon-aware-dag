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

    task_rdd_19 = simulate_task(base_rdd, '19', 91)
    task_rdd_11 = simulate_task(base_rdd, '11', 9)
    task_rdd_14 = simulate_task(base_rdd, '14', 59)
    task_rdd_132 = simulate_task(base_rdd, '132', 4)
    task_rdd_3 = simulate_task(base_rdd, '3', 419)
    task_rdd_2 = simulate_task(base_rdd, '2', 3)
    task_rdd_16 = simulate_task(base_rdd, '16', 102)
    task_rdd_22 = simulate_task(base_rdd, '22', 188)
    task_rdd_6 = simulate_task(base_rdd, '6', 1)
    task_rdd_120 = simulate_task(base_rdd, '120', 4)
    task_rdd_67 = simulate_task(base_rdd, '67', 3)
    task_rdd_27 = simulate_task(base_rdd, '27', 23)

    union_rdd_20 = spark.sparkContext.union([task_rdd_14, task_rdd_16, task_rdd_19])
    task_rdd_20 = simulate_task(union_rdd_20, '20', 9)

    union_rdd_15 = spark.sparkContext.union([task_rdd_2, task_rdd_14])
    task_rdd_15 = simulate_task(union_rdd_15, '15', 15)

    union_rdd_48 = spark.sparkContext.union([task_rdd_132])
    task_rdd_48 = simulate_task(union_rdd_48, '48', 4)

    union_rdd_7 = spark.sparkContext.union([task_rdd_67])
    task_rdd_7 = simulate_task(union_rdd_7, '7', 21)

    union_rdd_24 = spark.sparkContext.union([task_rdd_2])
    task_rdd_24 = simulate_task(union_rdd_24, '24', 2)

    union_rdd_17 = spark.sparkContext.union([task_rdd_16])
    task_rdd_17 = simulate_task(union_rdd_17, '17', 190)

    union_rdd_100 = spark.sparkContext.union([task_rdd_120])
    task_rdd_100 = simulate_task(union_rdd_100, '100', 4)

    union_rdd_31 = spark.sparkContext.union([task_rdd_27])
    task_rdd_31 = simulate_task(union_rdd_31, '31', 92)

    union_rdd_8 = spark.sparkContext.union([task_rdd_3, task_rdd_6, task_rdd_7])
    task_rdd_8 = simulate_task(union_rdd_8, '8', 21)

    union_rdd_9 = spark.sparkContext.union([])
    task_rdd_9 = simulate_task(union_rdd_9, '9', 8)

    union_rdd_26 = spark.sparkContext.union([])
    task_rdd_26 = simulate_task(union_rdd_26, '26', 11)

    union_rdd_10 = spark.sparkContext.union([task_rdd_2, task_rdd_9, task_rdd_15])
    task_rdd_10 = simulate_task(union_rdd_10, '10', 270)

    union_rdd_13 = spark.sparkContext.union([task_rdd_8])
    task_rdd_13 = simulate_task(union_rdd_13, '13', 3)

    union_rdd_12 = spark.sparkContext.union([task_rdd_2, task_rdd_3, task_rdd_6, task_rdd_10, task_rdd_11])
    task_rdd_12 = simulate_task(union_rdd_12, '12', 5)

    union_rdd_4 = spark.sparkContext.union([task_rdd_12])
    task_rdd_4 = simulate_task(union_rdd_4, '4', 4)

    union_rdd_1 = spark.sparkContext.union([task_rdd_3, task_rdd_4])
    task_rdd_1 = simulate_task(union_rdd_1, '1', 5)

    combined_rdd = spark.sparkContext.union([task_rdd_11, task_rdd_19, task_rdd_20])
    task_rdd = simulate_task(combined_rdd, 'J21_11_19_20', 217)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
