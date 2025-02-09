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

    task_rdd_11 = simulate_task(base_rdd, '11', 16)
    task_rdd_7 = simulate_task(base_rdd, '7', 1)
    task_rdd_8 = simulate_task(base_rdd, '8', 1)
    task_rdd_12 = simulate_task(base_rdd, '12', 2)
    task_rdd_6 = simulate_task(base_rdd, '6', 4)
    task_rdd_3 = simulate_task(base_rdd, '3', 4)
    task_rdd_59 = simulate_task(base_rdd, '59', 3)
    task_rdd_42 = simulate_task(base_rdd, '42', 50)
    task_rdd_13 = simulate_task(base_rdd, '13', 14)
    task_rdd_15 = simulate_task(base_rdd, '15', 42)
    task_rdd_45 = simulate_task(base_rdd, '45', 2)
    task_rdd_61 = simulate_task(base_rdd, '61', 18)
    task_rdd_27 = simulate_task(base_rdd, '27', 68)
    task_rdd_55 = simulate_task(base_rdd, '55', 192)

    union_rdd_4 = spark.sparkContext.union([task_rdd_55])
    task_rdd_4 = simulate_task(union_rdd_4, '4', 115)

    union_rdd_5 = spark.sparkContext.union([task_rdd_12])
    task_rdd_5 = simulate_task(union_rdd_5, '5', 22)

    union_rdd_44 = spark.sparkContext.union([task_rdd_59])
    task_rdd_44 = simulate_task(union_rdd_44, '44', 9)

    union_rdd_54 = spark.sparkContext.union([task_rdd_42])
    task_rdd_54 = simulate_task(union_rdd_54, '54', 10)

    union_rdd_46 = spark.sparkContext.union([task_rdd_45])
    task_rdd_46 = simulate_task(union_rdd_46, '46', 15)

    union_rdd_14 = spark.sparkContext.union([task_rdd_11, task_rdd_13])
    task_rdd_14 = simulate_task(union_rdd_14, '14', 438)

    union_rdd_16 = spark.sparkContext.union([task_rdd_15])
    task_rdd_16 = simulate_task(union_rdd_16, '16', 275)

    union_rdd_28 = spark.sparkContext.union([task_rdd_27])
    task_rdd_28 = simulate_task(union_rdd_28, '28', 25)

    union_rdd_9 = spark.sparkContext.union([task_rdd_4, task_rdd_7])
    task_rdd_9 = simulate_task(union_rdd_9, '9', 134)

    union_rdd_49 = spark.sparkContext.union([task_rdd_3, task_rdd_4])
    task_rdd_49 = simulate_task(union_rdd_49, '49', 17)

    union_rdd_1 = spark.sparkContext.union([task_rdd_44])
    task_rdd_1 = simulate_task(union_rdd_1, '1', 2)

    union_rdd_36 = spark.sparkContext.union([task_rdd_11, task_rdd_14])
    task_rdd_36 = simulate_task(union_rdd_36, '36', 645)

    union_rdd_18 = spark.sparkContext.union([task_rdd_16])
    task_rdd_18 = simulate_task(union_rdd_18, '18', 48)

    union_rdd_29 = spark.sparkContext.union([task_rdd_28])
    task_rdd_29 = simulate_task(union_rdd_29, '29', 481)

    union_rdd_10 = spark.sparkContext.union([task_rdd_3, task_rdd_6, task_rdd_7, task_rdd_9])
    task_rdd_10 = simulate_task(union_rdd_10, '10', 2567)

    union_rdd_2 = spark.sparkContext.union([task_rdd_1, task_rdd_6])
    task_rdd_2 = simulate_task(union_rdd_2, '2', 7)

    union_rdd_47 = spark.sparkContext.union([task_rdd_18, task_rdd_46])
    task_rdd_47 = simulate_task(union_rdd_47, '47', 4)

    union_rdd_73 = spark.sparkContext.union([task_rdd_36])
    task_rdd_73 = simulate_task(union_rdd_73, '73', 9)

    union_rdd_30 = spark.sparkContext.union([task_rdd_29])
    task_rdd_30 = simulate_task(union_rdd_30, '30', 86)

    union_rdd_48 = spark.sparkContext.union([task_rdd_46, task_rdd_47, task_rdd_54])
    task_rdd_48 = simulate_task(union_rdd_48, '48', 12)

    union_rdd_60 = spark.sparkContext.union([task_rdd_73])
    task_rdd_60 = simulate_task(union_rdd_60, '60', 5)

    union_rdd_64 = spark.sparkContext.union([task_rdd_48, task_rdd_49])
    task_rdd_64 = simulate_task(union_rdd_64, '64', 84)

    union_rdd_17 = spark.sparkContext.union([task_rdd_60])
    task_rdd_17 = simulate_task(union_rdd_17, '17', 2)

    union_rdd_62 = spark.sparkContext.union([task_rdd_61, task_rdd_64])
    task_rdd_62 = simulate_task(union_rdd_62, '62', 83)

    union_rdd_63 = spark.sparkContext.union([task_rdd_30, task_rdd_62])
    task_rdd_63 = simulate_task(union_rdd_63, '63', 92)

    combined_rdd = spark.sparkContext.union([task_rdd_4, task_rdd_7, task_rdd_8, task_rdd_10])
    task_rdd = simulate_task(combined_rdd, 'J11_4_7_8_10', 149)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
