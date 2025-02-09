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

    task_rdd_7 = simulate_task(base_rdd, '7', 218)
    task_rdd_16 = simulate_task(base_rdd, '16', 2)
    task_rdd_9 = simulate_task(base_rdd, '9', 1)
    task_rdd_10 = simulate_task(base_rdd, '10', 53)
    task_rdd_8 = simulate_task(base_rdd, '8', 112)
    task_rdd_5 = simulate_task(base_rdd, '5', 1)
    task_rdd_20 = simulate_task(base_rdd, '20', 28)
    task_rdd_11 = simulate_task(base_rdd, '11', 32)
    task_rdd_131 = simulate_task(base_rdd, '131', 4)
    task_rdd_21 = simulate_task(base_rdd, '21', 11)
    task_rdd_62 = simulate_task(base_rdd, '62', 8)
    task_rdd_38 = simulate_task(base_rdd, '38', 10)
    task_rdd_30 = simulate_task(base_rdd, '30', 3)
    task_rdd_28 = simulate_task(base_rdd, '28', 18)
    task_rdd_99 = simulate_task(base_rdd, '99', 3)
    task_rdd_100 = simulate_task(base_rdd, '100', 4)
    task_rdd_31 = simulate_task(base_rdd, '31', 26)
    task_rdd_47 = simulate_task(base_rdd, '47', 17)

    union_rdd_132 = spark.sparkContext.union([task_rdd_131])
    task_rdd_132 = simulate_task(union_rdd_132, '132', 1)

    union_rdd_22 = spark.sparkContext.union([task_rdd_21])
    task_rdd_22 = simulate_task(union_rdd_22, '22', 95)

    union_rdd_39 = spark.sparkContext.union([task_rdd_30, task_rdd_38])
    task_rdd_39 = simulate_task(union_rdd_39, '39', 7)

    union_rdd_35 = spark.sparkContext.union([task_rdd_100])
    task_rdd_35 = simulate_task(union_rdd_35, '35', 203)

    union_rdd_29 = spark.sparkContext.union([task_rdd_47])
    task_rdd_29 = simulate_task(union_rdd_29, '29', 51)

    union_rdd_83 = spark.sparkContext.union([task_rdd_99])
    task_rdd_83 = simulate_task(union_rdd_83, '83', 11)

    union_rdd_24 = spark.sparkContext.union([task_rdd_132])
    task_rdd_24 = simulate_task(union_rdd_24, '24', 4)

    union_rdd_40 = spark.sparkContext.union([task_rdd_29, task_rdd_31, task_rdd_35, task_rdd_39])
    task_rdd_40 = simulate_task(union_rdd_40, '40', 4)

    union_rdd_25 = spark.sparkContext.union([task_rdd_24])
    task_rdd_25 = simulate_task(union_rdd_25, '25', 70)

    union_rdd_36 = spark.sparkContext.union([task_rdd_38, task_rdd_40])
    task_rdd_36 = simulate_task(union_rdd_36, '36', 4)

    union_rdd_26 = spark.sparkContext.union([task_rdd_25])
    task_rdd_26 = simulate_task(union_rdd_26, '26', 11)

    union_rdd_37 = spark.sparkContext.union([task_rdd_36])
    task_rdd_37 = simulate_task(union_rdd_37, '37', 891)

    union_rdd_15 = spark.sparkContext.union([task_rdd_26])
    task_rdd_15 = simulate_task(union_rdd_15, '15', 282)

    union_rdd_32 = spark.sparkContext.union([task_rdd_31, task_rdd_37])
    task_rdd_32 = simulate_task(union_rdd_32, '32', 59)

    union_rdd_33 = spark.sparkContext.union([task_rdd_32])
    task_rdd_33 = simulate_task(union_rdd_33, '33', 27)

    union_rdd_34 = spark.sparkContext.union([task_rdd_28, task_rdd_32, task_rdd_33])
    task_rdd_34 = simulate_task(union_rdd_34, '34', 234)

    union_rdd_2 = spark.sparkContext.union([task_rdd_30, task_rdd_34])
    task_rdd_2 = simulate_task(union_rdd_2, '2', 17)

    union_rdd_6 = spark.sparkContext.union([task_rdd_2, task_rdd_5, task_rdd_7])
    task_rdd_6 = simulate_task(union_rdd_6, '6', 30)

    union_rdd_17 = spark.sparkContext.union([task_rdd_2, task_rdd_5, task_rdd_8, task_rdd_9, task_rdd_15, task_rdd_16])
    task_rdd_17 = simulate_task(union_rdd_17, '17', 372)

    union_rdd_23 = spark.sparkContext.union([task_rdd_6])
    task_rdd_23 = simulate_task(union_rdd_23, '23', 11)

    union_rdd_18 = spark.sparkContext.union([task_rdd_8, task_rdd_17])
    task_rdd_18 = simulate_task(union_rdd_18, '18', 9)

    union_rdd_13 = spark.sparkContext.union([task_rdd_5, task_rdd_8])
    task_rdd_13 = simulate_task(union_rdd_13, '13', 10)

    union_rdd_1 = spark.sparkContext.union([])
    task_rdd_1 = simulate_task(union_rdd_1, '1', 141)

    union_rdd_14 = spark.sparkContext.union([task_rdd_2, task_rdd_5, task_rdd_7, task_rdd_13])
    task_rdd_14 = simulate_task(union_rdd_14, '14', 1131)

    union_rdd_3 = spark.sparkContext.union([task_rdd_1])
    task_rdd_3 = simulate_task(union_rdd_3, '3', 2)

    union_rdd_4 = spark.sparkContext.union([task_rdd_2, task_rdd_3])
    task_rdd_4 = simulate_task(union_rdd_4, '4', 13)

    combined_rdd = spark.sparkContext.union([task_rdd_14])
    task_rdd = simulate_task(combined_rdd, 'R19_14', 30)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
