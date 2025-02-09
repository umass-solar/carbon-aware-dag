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

    task_rdd_49 = simulate_task(base_rdd, '49', 108)
    task_rdd_16 = simulate_task(base_rdd, '16', 210)
    task_rdd_9 = simulate_task(base_rdd, '9', 10)
    task_rdd_5 = simulate_task(base_rdd, '5', 10)
    task_rdd_2 = simulate_task(base_rdd, '2', 130)
    task_rdd_7 = simulate_task(base_rdd, '7', 59)
    task_rdd_12 = simulate_task(base_rdd, '12', 6)
    task_rdd_32 = simulate_task(base_rdd, '32', 31)
    task_rdd_22 = simulate_task(base_rdd, '22', 87)
    task_rdd_35 = simulate_task(base_rdd, '35', 2)
    task_rdd_54 = simulate_task(base_rdd, '54', 87)
    task_rdd_53 = simulate_task(base_rdd, '53', 108)
    task_rdd_131 = simulate_task(base_rdd, '131', 4)

    union_rdd_17 = spark.sparkContext.union([task_rdd_49])
    task_rdd_17 = simulate_task(union_rdd_17, '17', 2)

    union_rdd_8 = spark.sparkContext.union([task_rdd_7, task_rdd_16])
    task_rdd_8 = simulate_task(union_rdd_8, '8', 4)

    union_rdd_10 = spark.sparkContext.union([task_rdd_7, task_rdd_9])
    task_rdd_10 = simulate_task(union_rdd_10, '10', 8)

    union_rdd_18 = spark.sparkContext.union([task_rdd_12])
    task_rdd_18 = simulate_task(union_rdd_18, '18', 189)

    union_rdd_23 = spark.sparkContext.union([task_rdd_22])
    task_rdd_23 = simulate_task(union_rdd_23, '23', 6)

    union_rdd_97 = spark.sparkContext.union([task_rdd_35])
    task_rdd_97 = simulate_task(union_rdd_97, '97', 8)

    union_rdd_132 = spark.sparkContext.union([task_rdd_131])
    task_rdd_132 = simulate_task(union_rdd_132, '132', 1)

    union_rdd_6 = spark.sparkContext.union([task_rdd_5, task_rdd_8])
    task_rdd_6 = simulate_task(union_rdd_6, '6', 163)

    union_rdd_11 = spark.sparkContext.union([task_rdd_2, task_rdd_5, task_rdd_10])
    task_rdd_11 = simulate_task(union_rdd_11, '11', 56)

    union_rdd_1 = spark.sparkContext.union([task_rdd_23])
    task_rdd_1 = simulate_task(union_rdd_1, '1', 35)

    union_rdd_78 = spark.sparkContext.union([task_rdd_97])
    task_rdd_78 = simulate_task(union_rdd_78, '78', 3)

    union_rdd_51 = spark.sparkContext.union([task_rdd_132])
    task_rdd_51 = simulate_task(union_rdd_51, '51', 4)

    union_rdd_19 = spark.sparkContext.union([task_rdd_6, task_rdd_17])
    task_rdd_19 = simulate_task(union_rdd_19, '19', 473)

    union_rdd_15 = spark.sparkContext.union([task_rdd_11])
    task_rdd_15 = simulate_task(union_rdd_15, '15', 1)

    union_rdd_52 = spark.sparkContext.union([task_rdd_51])
    task_rdd_52 = simulate_task(union_rdd_52, '52', 1)

    union_rdd_55 = spark.sparkContext.union([task_rdd_52, task_rdd_53, task_rdd_54])
    task_rdd_55 = simulate_task(union_rdd_55, '55', 1)

    union_rdd_56 = spark.sparkContext.union([task_rdd_55, task_rdd_78])
    task_rdd_56 = simulate_task(union_rdd_56, '56', 12)

    union_rdd_57 = spark.sparkContext.union([task_rdd_56])
    task_rdd_57 = simulate_task(union_rdd_57, '57', 2)

    union_rdd_13 = spark.sparkContext.union([task_rdd_57])
    task_rdd_13 = simulate_task(union_rdd_13, '13', 6)

    union_rdd_20 = spark.sparkContext.union([task_rdd_13, task_rdd_16, task_rdd_19])
    task_rdd_20 = simulate_task(union_rdd_20, '20', 28)

    union_rdd_21 = spark.sparkContext.union([task_rdd_12, task_rdd_13, task_rdd_18])
    task_rdd_21 = simulate_task(union_rdd_21, '21', 230)

    union_rdd_14 = spark.sparkContext.union([task_rdd_9, task_rdd_13, task_rdd_20])
    task_rdd_14 = simulate_task(union_rdd_14, '14', 16)

    union_rdd_3 = spark.sparkContext.union([task_rdd_2, task_rdd_14])
    task_rdd_3 = simulate_task(union_rdd_3, '3', 2)

    union_rdd_4 = spark.sparkContext.union([task_rdd_2, task_rdd_3])
    task_rdd_4 = simulate_task(union_rdd_4, '4', 23)

    union_rdd_50 = spark.sparkContext.union([task_rdd_4])
    task_rdd_50 = simulate_task(union_rdd_50, '50', 32)

    combined_rdd = spark.sparkContext.union([task_rdd_50])
    task_rdd = simulate_task(combined_rdd, 'R33_50', 4)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
