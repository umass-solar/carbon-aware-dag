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

    task_rdd_18 = simulate_task(base_rdd, '18', 236)
    task_rdd_17 = simulate_task(base_rdd, '17', 20)
    task_rdd_16 = simulate_task(base_rdd, '16', 4)
    task_rdd_13 = simulate_task(base_rdd, '13', 396)
    task_rdd_8 = simulate_task(base_rdd, '8', 62)
    task_rdd_31 = simulate_task(base_rdd, '31', 14)
    task_rdd_29 = simulate_task(base_rdd, '29', 27)
    task_rdd_3 = simulate_task(base_rdd, '3', 1)
    task_rdd_7 = simulate_task(base_rdd, '7', 13)
    task_rdd_78 = simulate_task(base_rdd, '78', 4)
    task_rdd_49 = simulate_task(base_rdd, '49', 97)
    task_rdd_51 = simulate_task(base_rdd, '51', 3)
    task_rdd_24 = simulate_task(base_rdd, '24', 3)
    task_rdd_5 = simulate_task(base_rdd, '5', 5)
    task_rdd_11 = simulate_task(base_rdd, '11', 146)
    task_rdd_10 = simulate_task(base_rdd, '10', 34)
    task_rdd_2 = simulate_task(base_rdd, '2', 7)
    task_rdd_36 = simulate_task(base_rdd, '36', 2)
    task_rdd_25 = simulate_task(base_rdd, '25', 30)
    task_rdd_28 = simulate_task(base_rdd, '28', 22)
    task_rdd_26 = simulate_task(base_rdd, '26', 8)

    union_rdd_19 = spark.sparkContext.union([task_rdd_2, task_rdd_5, task_rdd_8, task_rdd_13, task_rdd_18])
    task_rdd_19 = simulate_task(union_rdd_19, '19', 21)

    union_rdd_30 = spark.sparkContext.union([task_rdd_29])
    task_rdd_30 = simulate_task(union_rdd_30, '30', 742)

    union_rdd_45 = spark.sparkContext.union([task_rdd_51])
    task_rdd_45 = simulate_task(union_rdd_45, '45', 2)

    union_rdd_37 = spark.sparkContext.union([task_rdd_36])
    task_rdd_37 = simulate_task(union_rdd_37, '37', 169)

    union_rdd_53 = spark.sparkContext.union([task_rdd_26, task_rdd_28])
    task_rdd_53 = simulate_task(union_rdd_53, '53', 12)

    union_rdd_20 = spark.sparkContext.union([task_rdd_53])
    task_rdd_20 = simulate_task(union_rdd_20, '20', 86)

    union_rdd_32 = spark.sparkContext.union([task_rdd_25, task_rdd_30, task_rdd_31])
    task_rdd_32 = simulate_task(union_rdd_32, '32', 81)

    union_rdd_46 = spark.sparkContext.union([task_rdd_45])
    task_rdd_46 = simulate_task(union_rdd_46, '46', 215)

    union_rdd_38 = spark.sparkContext.union([task_rdd_37])
    task_rdd_38 = simulate_task(union_rdd_38, '38', 138)

    union_rdd_21 = spark.sparkContext.union([task_rdd_11, task_rdd_13, task_rdd_16, task_rdd_17, task_rdd_18, task_rdd_20])
    task_rdd_21 = simulate_task(union_rdd_21, '21', 18)

    union_rdd_33 = spark.sparkContext.union([task_rdd_2, task_rdd_10, task_rdd_32])
    task_rdd_33 = simulate_task(union_rdd_33, '33', 68)

    union_rdd_47 = spark.sparkContext.union([task_rdd_46])
    task_rdd_47 = simulate_task(union_rdd_47, '47', 189)

    union_rdd_22 = spark.sparkContext.union([task_rdd_2, task_rdd_8, task_rdd_10, task_rdd_16, task_rdd_18, task_rdd_20, task_rdd_21])
    task_rdd_22 = simulate_task(union_rdd_22, '22', 25)

    union_rdd_34 = spark.sparkContext.union([task_rdd_33])
    task_rdd_34 = simulate_task(union_rdd_34, '34', 151)

    union_rdd_48 = spark.sparkContext.union([task_rdd_47, task_rdd_49])
    task_rdd_48 = simulate_task(union_rdd_48, '48', 5)

    union_rdd_41 = spark.sparkContext.union([task_rdd_22])
    task_rdd_41 = simulate_task(union_rdd_41, '41', 4)

    union_rdd_79 = spark.sparkContext.union([task_rdd_24, task_rdd_48, task_rdd_78])
    task_rdd_79 = simulate_task(union_rdd_79, '79', 25)

    union_rdd_42 = spark.sparkContext.union([task_rdd_41])
    task_rdd_42 = simulate_task(union_rdd_42, '42', 39)

    union_rdd_44 = spark.sparkContext.union([task_rdd_79])
    task_rdd_44 = simulate_task(union_rdd_44, '44', 2)

    union_rdd_9 = spark.sparkContext.union([task_rdd_42])
    task_rdd_9 = simulate_task(union_rdd_9, '9', 145)

    union_rdd_14 = spark.sparkContext.union([task_rdd_9])
    task_rdd_14 = simulate_task(union_rdd_14, '14', 3)

    union_rdd_12 = spark.sparkContext.union([task_rdd_14])
    task_rdd_12 = simulate_task(union_rdd_12, '12', 5)

    union_rdd_1 = spark.sparkContext.union([task_rdd_5, task_rdd_12])
    task_rdd_1 = simulate_task(union_rdd_1, '1', 2)

    union_rdd_6 = spark.sparkContext.union([task_rdd_1])
    task_rdd_6 = simulate_task(union_rdd_6, '6', 3)

    union_rdd_4 = spark.sparkContext.union([task_rdd_8])
    task_rdd_4 = simulate_task(union_rdd_4, '4', 57)

    combined_rdd = spark.sparkContext.union([task_rdd_4])
    task_rdd = simulate_task(combined_rdd, 'R15_4', 61)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
