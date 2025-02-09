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

    task_rdd_4 = simulate_task(base_rdd, '4', 27)
    task_rdd_5 = simulate_task(base_rdd, '5', 131)
    task_rdd_2 = simulate_task(base_rdd, '2', 12)
    task_rdd_3 = simulate_task(base_rdd, '3', 4)
    task_rdd_18 = simulate_task(base_rdd, '18', 20)
    task_rdd_77 = simulate_task(base_rdd, '77', 35)
    task_rdd_1 = simulate_task(base_rdd, '1', 26)
    task_rdd_6 = simulate_task(base_rdd, '6', 1)
    task_rdd_8 = simulate_task(base_rdd, '8', 16)
    task_rdd_43 = simulate_task(base_rdd, '43', 2)
    task_rdd_41 = simulate_task(base_rdd, '41', 21)
    task_rdd_50 = simulate_task(base_rdd, '50', 4)
    task_rdd_31 = simulate_task(base_rdd, '31', 6)
    task_rdd_32 = simulate_task(base_rdd, '32', 8)
    task_rdd_22 = simulate_task(base_rdd, '22', 68)
    task_rdd_14 = simulate_task(base_rdd, '14', 68)
    task_rdd_13 = simulate_task(base_rdd, '13', 19)
    task_rdd_23 = simulate_task(base_rdd, '23', 28)
    task_rdd_27 = simulate_task(base_rdd, '27', 5)
    task_rdd_26 = simulate_task(base_rdd, '26', 24)
    task_rdd_66 = simulate_task(base_rdd, '66', 32)
    task_rdd_38 = simulate_task(base_rdd, '38', 19)
    task_rdd_119 = simulate_task(base_rdd, '119', 4)

    union_rdd_9 = spark.sparkContext.union([task_rdd_4])
    task_rdd_9 = simulate_task(union_rdd_9, '9', 5)

    union_rdd_11 = spark.sparkContext.union([task_rdd_2, task_rdd_3, task_rdd_5])
    task_rdd_11 = simulate_task(union_rdd_11, '11', 16)

    union_rdd_34 = spark.sparkContext.union([task_rdd_1, task_rdd_2])
    task_rdd_34 = simulate_task(union_rdd_34, '34', 72)

    union_rdd_21 = spark.sparkContext.union([task_rdd_3])
    task_rdd_21 = simulate_task(union_rdd_21, '21', 10)

    union_rdd_58 = spark.sparkContext.union([task_rdd_77])
    task_rdd_58 = simulate_task(union_rdd_58, '58', 3)

    union_rdd_12 = spark.sparkContext.union([task_rdd_1])
    task_rdd_12 = simulate_task(union_rdd_12, '12', 208)

    union_rdd_44 = spark.sparkContext.union([task_rdd_41, task_rdd_43])
    task_rdd_44 = simulate_task(union_rdd_44, '44', 21)

    union_rdd_39 = spark.sparkContext.union([task_rdd_41])
    task_rdd_39 = simulate_task(union_rdd_39, '39', 319)

    union_rdd_33 = spark.sparkContext.union([task_rdd_32])
    task_rdd_33 = simulate_task(union_rdd_33, '33', 67)

    union_rdd_15 = spark.sparkContext.union([task_rdd_14])
    task_rdd_15 = simulate_task(union_rdd_15, '15', 34)

    union_rdd_28 = spark.sparkContext.union([task_rdd_27])
    task_rdd_28 = simulate_task(union_rdd_28, '28', 1)

    union_rdd_60 = spark.sparkContext.union([task_rdd_38])
    task_rdd_60 = simulate_task(union_rdd_60, '60', 1219)

    union_rdd_120 = spark.sparkContext.union([task_rdd_119])
    task_rdd_120 = simulate_task(union_rdd_120, '120', 275)

    union_rdd_7 = spark.sparkContext.union([task_rdd_2, task_rdd_5, task_rdd_6, task_rdd_11])
    task_rdd_7 = simulate_task(union_rdd_7, '7', 12)

    union_rdd_37 = spark.sparkContext.union([task_rdd_34])
    task_rdd_37 = simulate_task(union_rdd_37, '37', 96)

    union_rdd_19 = spark.sparkContext.union([task_rdd_18, task_rdd_21])
    task_rdd_19 = simulate_task(union_rdd_19, '19', 257)

    union_rdd_16 = spark.sparkContext.union([task_rdd_8, task_rdd_12])
    task_rdd_16 = simulate_task(union_rdd_16, '16', 2)

    union_rdd_42 = spark.sparkContext.union([task_rdd_60])
    task_rdd_42 = simulate_task(union_rdd_42, '42', 8)

    union_rdd_30 = spark.sparkContext.union([task_rdd_32, task_rdd_33])
    task_rdd_30 = simulate_task(union_rdd_30, '30', 2)

    union_rdd_25 = spark.sparkContext.union([task_rdd_13, task_rdd_14, task_rdd_15, task_rdd_22])
    task_rdd_25 = simulate_task(union_rdd_25, '25', 50)

    union_rdd_29 = spark.sparkContext.union([task_rdd_28])
    task_rdd_29 = simulate_task(union_rdd_29, '29', 5)

    union_rdd_89 = spark.sparkContext.union([task_rdd_120])
    task_rdd_89 = simulate_task(union_rdd_89, '89', 4)

    union_rdd_24 = spark.sparkContext.union([])
    task_rdd_24 = simulate_task(union_rdd_24, '24', 26)

    union_rdd_40 = spark.sparkContext.union([])
    task_rdd_40 = simulate_task(union_rdd_40, '40', 1)

    union_rdd_61 = spark.sparkContext.union([])
    task_rdd_61 = simulate_task(union_rdd_61, '61', 4)

    union_rdd_57 = spark.sparkContext.union([task_rdd_61])
    task_rdd_57 = simulate_task(union_rdd_57, '57', 2)

    union_rdd_17 = spark.sparkContext.union([task_rdd_57])
    task_rdd_17 = simulate_task(union_rdd_17, '17', 5)

    union_rdd_20 = spark.sparkContext.union([task_rdd_14, task_rdd_17, task_rdd_18, task_rdd_19])
    task_rdd_20 = simulate_task(union_rdd_20, '20', 239)

    combined_rdd = spark.sparkContext.union([task_rdd_4, task_rdd_5, task_rdd_9])
    task_rdd = simulate_task(combined_rdd, 'R10_4_5_9', 10)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
