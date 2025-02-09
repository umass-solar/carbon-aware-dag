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

    task_rdd_7 = simulate_task(base_rdd, '7', 2)
    task_rdd_4 = simulate_task(base_rdd, '4', 31)
    task_rdd_10 = simulate_task(base_rdd, '10', 5)
    task_rdd_2 = simulate_task(base_rdd, '2', 3)
    task_rdd_12 = simulate_task(base_rdd, '12', 11)
    task_rdd_3 = simulate_task(base_rdd, '3', 25)
    task_rdd_9 = simulate_task(base_rdd, '9', 142)
    task_rdd_1 = simulate_task(base_rdd, '1', 1)
    task_rdd_63 = simulate_task(base_rdd, '63', 143)
    task_rdd_49 = simulate_task(base_rdd, '49', 17)
    task_rdd_48 = simulate_task(base_rdd, '48', 28)
    task_rdd_53 = simulate_task(base_rdd, '53', 3)
    task_rdd_32 = simulate_task(base_rdd, '32', 1)
    task_rdd_46 = simulate_task(base_rdd, '46', 14)
    task_rdd_13 = simulate_task(base_rdd, '13', 8)
    task_rdd_21 = simulate_task(base_rdd, '21', 2)
    task_rdd_16 = simulate_task(base_rdd, '16', 5)
    task_rdd_36 = simulate_task(base_rdd, '36', 24)
    task_rdd_30 = simulate_task(base_rdd, '30', 11)
    task_rdd_52 = simulate_task(base_rdd, '52', 83)
    task_rdd_50 = simulate_task(base_rdd, '50', 18)
    task_rdd_24 = simulate_task(base_rdd, '24', 6)
    task_rdd_19 = simulate_task(base_rdd, '19', 34)

    union_rdd_6 = spark.sparkContext.union([task_rdd_2, task_rdd_4])
    task_rdd_6 = simulate_task(union_rdd_6, '6', 110)

    union_rdd_11 = spark.sparkContext.union([task_rdd_7, task_rdd_10])
    task_rdd_11 = simulate_task(union_rdd_11, '11', 18)

    union_rdd_5 = spark.sparkContext.union([task_rdd_1, task_rdd_3])
    task_rdd_5 = simulate_task(union_rdd_5, '5', 18)

    union_rdd_64 = spark.sparkContext.union([task_rdd_48, task_rdd_49, task_rdd_63])
    task_rdd_64 = simulate_task(union_rdd_64, '64', 84)

    union_rdd_54 = spark.sparkContext.union([task_rdd_50, task_rdd_52])
    task_rdd_54 = simulate_task(union_rdd_54, '54', 17)

    union_rdd_33 = spark.sparkContext.union([task_rdd_32])
    task_rdd_33 = simulate_task(union_rdd_33, '33', 97)

    union_rdd_20 = spark.sparkContext.union([task_rdd_13])
    task_rdd_20 = simulate_task(union_rdd_20, '20', 29)

    union_rdd_22 = spark.sparkContext.union([task_rdd_24])
    task_rdd_22 = simulate_task(union_rdd_22, '22', 2)

    union_rdd_17 = spark.sparkContext.union([task_rdd_16])
    task_rdd_17 = simulate_task(union_rdd_17, '17', 8)

    union_rdd_31 = spark.sparkContext.union([task_rdd_7, task_rdd_30])
    task_rdd_31 = simulate_task(union_rdd_31, '31', 552)

    union_rdd_15 = spark.sparkContext.union([task_rdd_1, task_rdd_2, task_rdd_3, task_rdd_7, task_rdd_10, task_rdd_11])
    task_rdd_15 = simulate_task(union_rdd_15, '15', 18)

    union_rdd_55 = spark.sparkContext.union([task_rdd_54])
    task_rdd_55 = simulate_task(union_rdd_55, '55', 1168)

    union_rdd_51 = spark.sparkContext.union([task_rdd_33])
    task_rdd_51 = simulate_task(union_rdd_51, '51', 640)

    union_rdd_39 = spark.sparkContext.union([task_rdd_31])
    task_rdd_39 = simulate_task(union_rdd_39, '39', 5)

    union_rdd_18 = spark.sparkContext.union([task_rdd_17])
    task_rdd_18 = simulate_task(union_rdd_18, '18', 8)

    union_rdd_37 = spark.sparkContext.union([task_rdd_15])
    task_rdd_37 = simulate_task(union_rdd_37, '37', 17)

    union_rdd_56 = spark.sparkContext.union([task_rdd_55])
    task_rdd_56 = simulate_task(union_rdd_56, '56', 81)

    union_rdd_40 = spark.sparkContext.union([task_rdd_22, task_rdd_39])
    task_rdd_40 = simulate_task(union_rdd_40, '40', 45)

    union_rdd_38 = spark.sparkContext.union([task_rdd_37])
    task_rdd_38 = simulate_task(union_rdd_38, '38', 19)

    union_rdd_57 = spark.sparkContext.union([task_rdd_22, task_rdd_54, task_rdd_56])
    task_rdd_57 = simulate_task(union_rdd_57, '57', 63)

    union_rdd_41 = spark.sparkContext.union([task_rdd_40])
    task_rdd_41 = simulate_task(union_rdd_41, '41', 8)

    union_rdd_25 = spark.sparkContext.union([task_rdd_38])
    task_rdd_25 = simulate_task(union_rdd_25, '25', 7)

    union_rdd_58 = spark.sparkContext.union([task_rdd_57, task_rdd_64])
    task_rdd_58 = simulate_task(union_rdd_58, '58', 82)

    union_rdd_42 = spark.sparkContext.union([task_rdd_39, task_rdd_41])
    task_rdd_42 = simulate_task(union_rdd_42, '42', 20)

    union_rdd_26 = spark.sparkContext.union([task_rdd_16, task_rdd_25])
    task_rdd_26 = simulate_task(union_rdd_26, '26', 8)

    union_rdd_47 = spark.sparkContext.union([task_rdd_46, task_rdd_58])
    task_rdd_47 = simulate_task(union_rdd_47, '47', 214)

    union_rdd_43 = spark.sparkContext.union([task_rdd_36, task_rdd_39, task_rdd_42])
    task_rdd_43 = simulate_task(union_rdd_43, '43', 72)

    union_rdd_8 = spark.sparkContext.union([task_rdd_3, task_rdd_7, task_rdd_26])
    task_rdd_8 = simulate_task(union_rdd_8, '8', 43)

    union_rdd_44 = spark.sparkContext.union([task_rdd_43])
    task_rdd_44 = simulate_task(union_rdd_44, '44', 14)

    union_rdd_14 = spark.sparkContext.union([task_rdd_7, task_rdd_8])
    task_rdd_14 = simulate_task(union_rdd_14, '14', 131)

    combined_rdd = spark.sparkContext.union([task_rdd_2, task_rdd_4, task_rdd_6])
    task_rdd = simulate_task(combined_rdd, 'J7_2_4_6', 82)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
