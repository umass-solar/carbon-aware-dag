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

    task_rdd_4 = simulate_task(base_rdd, '4', 19)
    task_rdd_13 = simulate_task(base_rdd, '13', 16)
    task_rdd_11 = simulate_task(base_rdd, '11', 11)
    task_rdd_6 = simulate_task(base_rdd, '6', 11)
    task_rdd_120 = simulate_task(base_rdd, '120', 6)
    task_rdd_5 = simulate_task(base_rdd, '5', 1)
    task_rdd_3 = simulate_task(base_rdd, '3', 29)
    task_rdd_8 = simulate_task(base_rdd, '8', 3)
    task_rdd_31 = simulate_task(base_rdd, '31', 66)
    task_rdd_35 = simulate_task(base_rdd, '35', 26)
    task_rdd_20 = simulate_task(base_rdd, '20', 11)
    task_rdd_19 = simulate_task(base_rdd, '19', 26)
    task_rdd_33 = simulate_task(base_rdd, '33', 241)
    task_rdd_40 = simulate_task(base_rdd, '40', 498)
    task_rdd_18 = simulate_task(base_rdd, '18', 214)
    task_rdd_132 = simulate_task(base_rdd, '132', 4)

    union_rdd_14 = spark.sparkContext.union([task_rdd_6, task_rdd_13])
    task_rdd_14 = simulate_task(union_rdd_14, '14', 17)

    union_rdd_42 = spark.sparkContext.union([task_rdd_120])
    task_rdd_42 = simulate_task(union_rdd_42, '42', 4)

    union_rdd_1 = spark.sparkContext.union([task_rdd_132])
    task_rdd_1 = simulate_task(union_rdd_1, '1', 9)

    union_rdd_21 = spark.sparkContext.union([task_rdd_20])
    task_rdd_21 = simulate_task(union_rdd_21, '21', 5)

    union_rdd_17 = spark.sparkContext.union([task_rdd_33])
    task_rdd_17 = simulate_task(union_rdd_17, '17', 326)

    union_rdd_27 = spark.sparkContext.union([task_rdd_18])
    task_rdd_27 = simulate_task(union_rdd_27, '27', 647)

    union_rdd_44 = spark.sparkContext.union([task_rdd_40])
    task_rdd_44 = simulate_task(union_rdd_44, '44', 4)

    union_rdd_15 = spark.sparkContext.union([task_rdd_1, task_rdd_3, task_rdd_4, task_rdd_11, task_rdd_14])
    task_rdd_15 = simulate_task(union_rdd_15, '15', 11)

    union_rdd_7 = spark.sparkContext.union([task_rdd_20, task_rdd_21])
    task_rdd_7 = simulate_task(union_rdd_7, '7', 33)

    union_rdd_32 = spark.sparkContext.union([task_rdd_42])
    task_rdd_32 = simulate_task(union_rdd_32, '32', 4)

    union_rdd_45 = spark.sparkContext.union([task_rdd_44])
    task_rdd_45 = simulate_task(union_rdd_45, '45', 234)

    union_rdd_16 = spark.sparkContext.union([task_rdd_5, task_rdd_8, task_rdd_11, task_rdd_13, task_rdd_15])
    task_rdd_16 = simulate_task(union_rdd_16, '16', 18)

    union_rdd_12 = spark.sparkContext.union([task_rdd_5, task_rdd_7, task_rdd_8, task_rdd_11])
    task_rdd_12 = simulate_task(union_rdd_12, '12', 3)

    union_rdd_10 = spark.sparkContext.union([task_rdd_3, task_rdd_5, task_rdd_7])
    task_rdd_10 = simulate_task(union_rdd_10, '10', 1)

    union_rdd_25 = spark.sparkContext.union([task_rdd_45])
    task_rdd_25 = simulate_task(union_rdd_25, '25', 10)

    union_rdd_60 = spark.sparkContext.union([task_rdd_16])
    task_rdd_60 = simulate_task(union_rdd_60, '60', 5)

    union_rdd_26 = spark.sparkContext.union([task_rdd_25])
    task_rdd_26 = simulate_task(union_rdd_26, '26', 188)

    union_rdd_30 = spark.sparkContext.union([task_rdd_27, task_rdd_60])
    task_rdd_30 = simulate_task(union_rdd_30, '30', 40)

    union_rdd_28 = spark.sparkContext.union([task_rdd_30])
    task_rdd_28 = simulate_task(union_rdd_28, '28', 3)

    union_rdd_9 = spark.sparkContext.union([task_rdd_8, task_rdd_28])
    task_rdd_9 = simulate_task(union_rdd_9, '9', 8)

    union_rdd_2 = spark.sparkContext.union([task_rdd_9])
    task_rdd_2 = simulate_task(union_rdd_2, '2', 163)

    combined_rdd = spark.sparkContext.union([task_rdd_1, task_rdd_2])
    task_rdd = simulate_task(combined_rdd, 'M4_1_2', 2)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
