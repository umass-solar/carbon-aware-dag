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

    task_rdd_39 = simulate_task(base_rdd, '39', 5)
    task_rdd_132 = simulate_task(base_rdd, '132', 4)
    task_rdd_8 = simulate_task(base_rdd, '8', 8)
    task_rdd_16 = simulate_task(base_rdd, '16', 4)
    task_rdd_11 = simulate_task(base_rdd, '11', 5)
    task_rdd_10 = simulate_task(base_rdd, '10', 17)
    task_rdd_6 = simulate_task(base_rdd, '6', 6)
    task_rdd_4 = simulate_task(base_rdd, '4', 2)
    task_rdd_1 = simulate_task(base_rdd, '1', 176)
    task_rdd_23 = simulate_task(base_rdd, '23', 202)
    task_rdd_20 = simulate_task(base_rdd, '20', 19)
    task_rdd_12 = simulate_task(base_rdd, '12', 6)
    task_rdd_3 = simulate_task(base_rdd, '3', 14)
    task_rdd_7 = simulate_task(base_rdd, '7', 37)
    task_rdd_5 = simulate_task(base_rdd, '5', 116)
    task_rdd_2 = simulate_task(base_rdd, '2', 6)
    task_rdd_31 = simulate_task(base_rdd, '31', 16)
    task_rdd_72 = simulate_task(base_rdd, '72', 113)
    task_rdd_54 = simulate_task(base_rdd, '54', 32)
    task_rdd_29 = simulate_task(base_rdd, '29', 5)
    task_rdd_56 = simulate_task(base_rdd, '56', 2)
    task_rdd_44 = simulate_task(base_rdd, '44', 11)
    task_rdd_43 = simulate_task(base_rdd, '43', 28)

    union_rdd_40 = spark.sparkContext.union([task_rdd_132])
    task_rdd_40 = simulate_task(union_rdd_40, '40', 4)

    union_rdd_9 = spark.sparkContext.union([task_rdd_2, task_rdd_3, task_rdd_5, task_rdd_6, task_rdd_8])
    task_rdd_9 = simulate_task(union_rdd_9, '9', 30)

    union_rdd_17 = spark.sparkContext.union([task_rdd_2, task_rdd_4, task_rdd_16])
    task_rdd_17 = simulate_task(union_rdd_17, '17', 496)

    union_rdd_14 = spark.sparkContext.union([task_rdd_2, task_rdd_4, task_rdd_11])
    task_rdd_14 = simulate_task(union_rdd_14, '14', 6)

    union_rdd_33 = spark.sparkContext.union([task_rdd_1, task_rdd_2, task_rdd_3, task_rdd_5, task_rdd_7, task_rdd_12, task_rdd_1])
    task_rdd_33 = simulate_task(union_rdd_33, '33', 11)

    union_rdd_26 = spark.sparkContext.union([task_rdd_2, task_rdd_5, task_rdd_20, task_rdd_23])
    task_rdd_26 = simulate_task(union_rdd_26, '26', 13)

    union_rdd_32 = spark.sparkContext.union([task_rdd_31])
    task_rdd_32 = simulate_task(union_rdd_32, '32', 5)

    union_rdd_55 = spark.sparkContext.union([task_rdd_54])
    task_rdd_55 = simulate_task(union_rdd_55, '55', 7)

    union_rdd_45 = spark.sparkContext.union([task_rdd_44])
    task_rdd_45 = simulate_task(union_rdd_45, '45', 31)

    union_rdd_42 = spark.sparkContext.union([task_rdd_40])
    task_rdd_42 = simulate_task(union_rdd_42, '42', 5)

    union_rdd_15 = spark.sparkContext.union([task_rdd_3, task_rdd_4, task_rdd_5, task_rdd_6, task_rdd_10, task_rdd_11, task_rdd_12, task_rdd_14])
    task_rdd_15 = simulate_task(union_rdd_15, '15', 8)

    union_rdd_34 = spark.sparkContext.union([task_rdd_12, task_rdd_33])
    task_rdd_34 = simulate_task(union_rdd_34, '34', 12)

    union_rdd_27 = spark.sparkContext.union([task_rdd_2, task_rdd_5, task_rdd_7, task_rdd_26])
    task_rdd_27 = simulate_task(union_rdd_27, '27', 42)

    union_rdd_46 = spark.sparkContext.union([task_rdd_45])
    task_rdd_46 = simulate_task(union_rdd_46, '46', 100)

    union_rdd_25 = spark.sparkContext.union([task_rdd_42])
    task_rdd_25 = simulate_task(union_rdd_25, '25', 5)

    union_rdd_24 = spark.sparkContext.union([task_rdd_23, task_rdd_34])
    task_rdd_24 = simulate_task(union_rdd_24, '24', 6)

    union_rdd_28 = spark.sparkContext.union([task_rdd_5, task_rdd_7, task_rdd_27])
    task_rdd_28 = simulate_task(union_rdd_28, '28', 379)

    union_rdd_47 = spark.sparkContext.union([task_rdd_44, task_rdd_46])
    task_rdd_47 = simulate_task(union_rdd_47, '47', 2)

    union_rdd_21 = spark.sparkContext.union([task_rdd_25])
    task_rdd_21 = simulate_task(union_rdd_21, '21', 28)

    union_rdd_13 = spark.sparkContext.union([task_rdd_24])
    task_rdd_13 = simulate_task(union_rdd_13, '13', 3)

    union_rdd_22 = spark.sparkContext.union([task_rdd_9, task_rdd_21])
    task_rdd_22 = simulate_task(union_rdd_22, '22', 13)

    union_rdd_18 = spark.sparkContext.union([task_rdd_13])
    task_rdd_18 = simulate_task(union_rdd_18, '18', 181)

    union_rdd_41 = spark.sparkContext.union([task_rdd_22])
    task_rdd_41 = simulate_task(union_rdd_41, '41', 4)

    union_rdd_19 = spark.sparkContext.union([task_rdd_3, task_rdd_5, task_rdd_12, task_rdd_18])
    task_rdd_19 = simulate_task(union_rdd_19, '19', 109)

    union_rdd_100 = spark.sparkContext.union([task_rdd_41])
    task_rdd_100 = simulate_task(union_rdd_100, '100', 152)

    union_rdd_49 = spark.sparkContext.union([task_rdd_100])
    task_rdd_49 = simulate_task(union_rdd_49, '49', 15)

    union_rdd_50 = spark.sparkContext.union([task_rdd_5, task_rdd_8, task_rdd_10, task_rdd_49])
    task_rdd_50 = simulate_task(union_rdd_50, '50', 266)

    union_rdd_51 = spark.sparkContext.union([task_rdd_47, task_rdd_50])
    task_rdd_51 = simulate_task(union_rdd_51, '51', 120)

    union_rdd_36 = spark.sparkContext.union([task_rdd_51])
    task_rdd_36 = simulate_task(union_rdd_36, '36', 3)

    union_rdd_37 = spark.sparkContext.union([task_rdd_2, task_rdd_3, task_rdd_36])
    task_rdd_37 = simulate_task(union_rdd_37, '37', 594)

    union_rdd_38 = spark.sparkContext.union([task_rdd_2, task_rdd_4, task_rdd_6, task_rdd_37])
    task_rdd_38 = simulate_task(union_rdd_38, '38', 3)

    combined_rdd = spark.sparkContext.union([task_rdd_31, task_rdd_38])
    task_rdd = simulate_task(combined_rdd, 'J39_31_38', 240)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
