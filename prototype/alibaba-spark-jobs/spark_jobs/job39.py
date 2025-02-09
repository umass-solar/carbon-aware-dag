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

    task_rdd_62 = simulate_task(base_rdd, '62', 11)
    task_rdd_37 = simulate_task(base_rdd, '37', 61)
    task_rdd_14 = simulate_task(base_rdd, '14', 17)
    task_rdd_13 = simulate_task(base_rdd, '13', 29)
    task_rdd_12 = simulate_task(base_rdd, '12', 71)
    task_rdd_11 = simulate_task(base_rdd, '11', 143)
    task_rdd_10 = simulate_task(base_rdd, '10', 86)
    task_rdd_3 = simulate_task(base_rdd, '3', 40)
    task_rdd_1 = simulate_task(base_rdd, '1', 6)
    task_rdd_5 = simulate_task(base_rdd, '5', 7)
    task_rdd_7 = simulate_task(base_rdd, '7', 16)
    task_rdd_20 = simulate_task(base_rdd, '20', 1)
    task_rdd_6 = simulate_task(base_rdd, '6', 8)
    task_rdd_28 = simulate_task(base_rdd, '28', 36)
    task_rdd_8 = simulate_task(base_rdd, '8', 3)
    task_rdd_25 = simulate_task(base_rdd, '25', 5)
    task_rdd_41 = simulate_task(base_rdd, '41', 8)
    task_rdd_27 = simulate_task(base_rdd, '27', 12)
    task_rdd_47 = simulate_task(base_rdd, '47', 7)

    union_rdd_55 = spark.sparkContext.union([task_rdd_62])
    task_rdd_55 = simulate_task(union_rdd_55, '55', 53)

    union_rdd_15 = spark.sparkContext.union([task_rdd_11, task_rdd_13, task_rdd_14])
    task_rdd_15 = simulate_task(union_rdd_15, '15', 25)

    union_rdd_29 = spark.sparkContext.union([task_rdd_8, task_rdd_28])
    task_rdd_29 = simulate_task(union_rdd_29, '29', 245)

    union_rdd_22 = spark.sparkContext.union([task_rdd_41])
    task_rdd_22 = simulate_task(union_rdd_22, '22', 86)

    union_rdd_21 = spark.sparkContext.union([task_rdd_47])
    task_rdd_21 = simulate_task(union_rdd_21, '21', 3)

    union_rdd_9 = spark.sparkContext.union([task_rdd_55])
    task_rdd_9 = simulate_task(union_rdd_9, '9', 13)

    union_rdd_38 = spark.sparkContext.union([task_rdd_7, task_rdd_14, task_rdd_21, task_rdd_28, task_rdd_37])
    task_rdd_38 = simulate_task(union_rdd_38, '38', 8)

    union_rdd_16 = spark.sparkContext.union([task_rdd_8, task_rdd_10, task_rdd_12, task_rdd_13, task_rdd_14, task_rdd_15])
    task_rdd_16 = simulate_task(union_rdd_16, '16', 19)

    union_rdd_26 = spark.sparkContext.union([task_rdd_25, task_rdd_29])
    task_rdd_26 = simulate_task(union_rdd_26, '26', 57)

    union_rdd_23 = spark.sparkContext.union([task_rdd_22])
    task_rdd_23 = simulate_task(union_rdd_23, '23', 26)

    union_rdd_39 = spark.sparkContext.union([task_rdd_8, task_rdd_10, task_rdd_38])
    task_rdd_39 = simulate_task(union_rdd_39, '39', 63)

    union_rdd_17 = spark.sparkContext.union([task_rdd_8, task_rdd_10, task_rdd_12, task_rdd_13, task_rdd_16])
    task_rdd_17 = simulate_task(union_rdd_17, '17', 4)

    union_rdd_24 = spark.sparkContext.union([task_rdd_22, task_rdd_23, task_rdd_26])
    task_rdd_24 = simulate_task(union_rdd_24, '24', 17)

    union_rdd_40 = spark.sparkContext.union([task_rdd_37, task_rdd_39])
    task_rdd_40 = simulate_task(union_rdd_40, '40', 6)

    union_rdd_18 = spark.sparkContext.union([task_rdd_1, task_rdd_5, task_rdd_17])
    task_rdd_18 = simulate_task(union_rdd_18, '18', 5)

    union_rdd_2 = spark.sparkContext.union([task_rdd_1, task_rdd_9])
    task_rdd_2 = simulate_task(union_rdd_2, '2', 5)

    union_rdd_34 = spark.sparkContext.union([])
    task_rdd_34 = simulate_task(union_rdd_34, '34', 2)

    union_rdd_35 = spark.sparkContext.union([task_rdd_25, task_rdd_27, task_rdd_34])
    task_rdd_35 = simulate_task(union_rdd_35, '35', 9)

    union_rdd_36 = spark.sparkContext.union([task_rdd_27, task_rdd_35])
    task_rdd_36 = simulate_task(union_rdd_36, '36', 19)

    combined_rdd = spark.sparkContext.union([task_rdd_2])
    task_rdd = simulate_task(combined_rdd, 'R19_2', 12)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
