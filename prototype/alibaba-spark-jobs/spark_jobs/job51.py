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

    task_rdd_12 = simulate_task(base_rdd, '12', 159)
    task_rdd_2 = simulate_task(base_rdd, '2', 3)
    task_rdd_36 = simulate_task(base_rdd, '36', 2)
    task_rdd_8 = simulate_task(base_rdd, '8', 13)
    task_rdd_10 = simulate_task(base_rdd, '10', 81)
    task_rdd_7 = simulate_task(base_rdd, '7', 79)
    task_rdd_6 = simulate_task(base_rdd, '6', 120)
    task_rdd_15 = simulate_task(base_rdd, '15', 235)
    task_rdd_17 = simulate_task(base_rdd, '17', 2)
    task_rdd_16 = simulate_task(base_rdd, '16', 11)
    task_rdd_5 = simulate_task(base_rdd, '5', 7)
    task_rdd_4 = simulate_task(base_rdd, '4', 26)
    task_rdd_1 = simulate_task(base_rdd, '1', 14)
    task_rdd_23 = simulate_task(base_rdd, '23', 202)
    task_rdd_20 = simulate_task(base_rdd, '20', 2)
    task_rdd_18 = simulate_task(base_rdd, '18', 3)
    task_rdd_56 = simulate_task(base_rdd, '56', 116)
    task_rdd_41 = simulate_task(base_rdd, '41', 5)
    task_rdd_11 = simulate_task(base_rdd, '11', 35)
    task_rdd_48 = simulate_task(base_rdd, '48', 4)
    task_rdd_32 = simulate_task(base_rdd, '32', 9)
    task_rdd_28 = simulate_task(base_rdd, '28', 23)
    task_rdd_40 = simulate_task(base_rdd, '40', 11)

    union_rdd_13 = spark.sparkContext.union([task_rdd_4, task_rdd_5, task_rdd_8, task_rdd_10, task_rdd_12])
    task_rdd_13 = simulate_task(union_rdd_13, '13', 276)

    union_rdd_21 = spark.sparkContext.union([task_rdd_2])
    task_rdd_21 = simulate_task(union_rdd_21, '21', 2)

    union_rdd_3 = spark.sparkContext.union([task_rdd_8])
    task_rdd_3 = simulate_task(union_rdd_3, '3', 17)

    union_rdd_19 = spark.sparkContext.union([task_rdd_15])
    task_rdd_19 = simulate_task(union_rdd_19, '19', 3)

    union_rdd_57 = spark.sparkContext.union([task_rdd_41, task_rdd_56])
    task_rdd_57 = simulate_task(union_rdd_57, '57', 144)

    union_rdd_33 = spark.sparkContext.union([task_rdd_32])
    task_rdd_33 = simulate_task(union_rdd_33, '33', 89)

    union_rdd_14 = spark.sparkContext.union([task_rdd_4, task_rdd_5, task_rdd_7, task_rdd_12, task_rdd_13])
    task_rdd_14 = simulate_task(union_rdd_14, '14', 12)

    union_rdd_22 = spark.sparkContext.union([task_rdd_2, task_rdd_4, task_rdd_6, task_rdd_8, task_rdd_10, task_rdd_16, task_rdd_18, task_rdd_20, task_rdd_21])
    task_rdd_22 = simulate_task(union_rdd_22, '22', 25)

    union_rdd_9 = spark.sparkContext.union([task_rdd_3, task_rdd_36])
    task_rdd_9 = simulate_task(union_rdd_9, '9', 81)

    union_rdd_27 = spark.sparkContext.union([task_rdd_17, task_rdd_19])
    task_rdd_27 = simulate_task(union_rdd_27, '27', 23)

    union_rdd_34 = spark.sparkContext.union([task_rdd_33])
    task_rdd_34 = simulate_task(union_rdd_34, '34', 68)

    union_rdd_25 = spark.sparkContext.union([task_rdd_16, task_rdd_17, task_rdd_18, task_rdd_20, task_rdd_23])
    task_rdd_25 = simulate_task(union_rdd_25, '25', 39)

    union_rdd_30 = spark.sparkContext.union([])
    task_rdd_30 = simulate_task(union_rdd_30, '30', 6)

    union_rdd_26 = spark.sparkContext.union([task_rdd_1, task_rdd_4, task_rdd_25])
    task_rdd_26 = simulate_task(union_rdd_26, '26', 32)

    union_rdd_31 = spark.sparkContext.union([task_rdd_28, task_rdd_30])
    task_rdd_31 = simulate_task(union_rdd_31, '31', 20)

    combined_rdd = spark.sparkContext.union([task_rdd_14])
    task_rdd = simulate_task(combined_rdd, 'R50_14', 31)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
