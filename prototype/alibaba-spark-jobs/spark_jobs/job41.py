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

    task_rdd_48 = simulate_task(base_rdd, '48', 34)
    task_rdd_39 = simulate_task(base_rdd, '39', 1)
    task_rdd_8 = simulate_task(base_rdd, '8', 5)
    task_rdd_3 = simulate_task(base_rdd, '3', 10)
    task_rdd_6 = simulate_task(base_rdd, '6', 5)
    task_rdd_4 = simulate_task(base_rdd, '4', 86)
    task_rdd_28 = simulate_task(base_rdd, '28', 168)
    task_rdd_12 = simulate_task(base_rdd, '12', 1)
    task_rdd_5 = simulate_task(base_rdd, '5', 2)
    task_rdd_10 = simulate_task(base_rdd, '10', 6)
    task_rdd_13 = simulate_task(base_rdd, '13', 12)
    task_rdd_11 = simulate_task(base_rdd, '11', 32)
    task_rdd_27 = simulate_task(base_rdd, '27', 18)
    task_rdd_49 = simulate_task(base_rdd, '49', 50)
    task_rdd_21 = simulate_task(base_rdd, '21', 8)
    task_rdd_18 = simulate_task(base_rdd, '18', 85)
    task_rdd_16 = simulate_task(base_rdd, '16', 27)
    task_rdd_76 = simulate_task(base_rdd, '76', 21)

    union_rdd_24 = spark.sparkContext.union([task_rdd_39])
    task_rdd_24 = simulate_task(union_rdd_24, '24', 23)

    union_rdd_22 = spark.sparkContext.union([task_rdd_18, task_rdd_21])
    task_rdd_22 = simulate_task(union_rdd_22, '22', 2)

    union_rdd_14 = spark.sparkContext.union([task_rdd_13])
    task_rdd_14 = simulate_task(union_rdd_14, '14', 553)

    union_rdd_17 = spark.sparkContext.union([task_rdd_16])
    task_rdd_17 = simulate_task(union_rdd_17, '17', 19)

    union_rdd_77 = spark.sparkContext.union([task_rdd_76])
    task_rdd_77 = simulate_task(union_rdd_77, '77', 855)

    union_rdd_15 = spark.sparkContext.union([task_rdd_11, task_rdd_14])
    task_rdd_15 = simulate_task(union_rdd_15, '15', 13)

    union_rdd_61 = spark.sparkContext.union([task_rdd_77])
    task_rdd_61 = simulate_task(union_rdd_61, '61', 2)

    union_rdd_2 = spark.sparkContext.union([task_rdd_61])
    task_rdd_2 = simulate_task(union_rdd_2, '2', 160)

    union_rdd_9 = spark.sparkContext.union([task_rdd_2, task_rdd_4, task_rdd_5, task_rdd_6, task_rdd_8])
    task_rdd_9 = simulate_task(union_rdd_9, '9', 24)

    union_rdd_23 = spark.sparkContext.union([task_rdd_8, task_rdd_9, task_rdd_22])
    task_rdd_23 = simulate_task(union_rdd_23, '23', 329)

    union_rdd_26 = spark.sparkContext.union([task_rdd_16, task_rdd_22, task_rdd_23, task_rdd_24, task_rdd_48])
    task_rdd_26 = simulate_task(union_rdd_26, '26', 283)

    union_rdd_25 = spark.sparkContext.union([task_rdd_26])
    task_rdd_25 = simulate_task(union_rdd_25, '25', 23)

    union_rdd_19 = spark.sparkContext.union([task_rdd_18, task_rdd_25])
    task_rdd_19 = simulate_task(union_rdd_19, '19', 232)

    union_rdd_20 = spark.sparkContext.union([task_rdd_2, task_rdd_3, task_rdd_19])
    task_rdd_20 = simulate_task(union_rdd_20, '20', 9)

    union_rdd_7 = spark.sparkContext.union([task_rdd_5, task_rdd_6, task_rdd_20])
    task_rdd_7 = simulate_task(union_rdd_7, '7', 16)

    union_rdd_1 = spark.sparkContext.union([task_rdd_4, task_rdd_5, task_rdd_7])
    task_rdd_1 = simulate_task(union_rdd_1, '1', 35)

    union_rdd_47 = spark.sparkContext.union([task_rdd_1])
    task_rdd_47 = simulate_task(union_rdd_47, '47', 161)

    combined_rdd = spark.sparkContext.union([task_rdd_47])
    task_rdd = simulate_task(combined_rdd, 'R44_47', 2)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
