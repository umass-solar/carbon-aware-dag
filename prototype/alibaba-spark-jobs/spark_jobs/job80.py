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

    task_rdd_6 = simulate_task(base_rdd, '6', 7)
    task_rdd_3 = simulate_task(base_rdd, '3', 5)
    task_rdd_5 = simulate_task(base_rdd, '5', 8)
    task_rdd_1 = simulate_task(base_rdd, '1', 109)
    task_rdd_32 = simulate_task(base_rdd, '32', 3)
    task_rdd_42 = simulate_task(base_rdd, '42', 50)
    task_rdd_39 = simulate_task(base_rdd, '39', 15)
    task_rdd_37 = simulate_task(base_rdd, '37', 66)
    task_rdd_30 = simulate_task(base_rdd, '30', 3)
    task_rdd_29 = simulate_task(base_rdd, '29', 68)
    task_rdd_17 = simulate_task(base_rdd, '17', 42)
    task_rdd_16 = simulate_task(base_rdd, '16', 8)
    task_rdd_12 = simulate_task(base_rdd, '12', 10)
    task_rdd_11 = simulate_task(base_rdd, '11', 91)
    task_rdd_53 = simulate_task(base_rdd, '53', 162)
    task_rdd_54 = simulate_task(base_rdd, '54', 15)
    task_rdd_23 = simulate_task(base_rdd, '23', 165)
    task_rdd_21 = simulate_task(base_rdd, '21', 11)
    task_rdd_72 = simulate_task(base_rdd, '72', 4)
    task_rdd_47 = simulate_task(base_rdd, '47', 8)
    task_rdd_36 = simulate_task(base_rdd, '36', 6)
    task_rdd_27 = simulate_task(base_rdd, '27', 7)
    task_rdd_44 = simulate_task(base_rdd, '44', 193)
    task_rdd_48 = simulate_task(base_rdd, '48', 3)

    union_rdd_8 = spark.sparkContext.union([task_rdd_1, task_rdd_6])
    task_rdd_8 = simulate_task(union_rdd_8, '8', 19)

    union_rdd_4 = spark.sparkContext.union([task_rdd_3])
    task_rdd_4 = simulate_task(union_rdd_4, '4', 2)

    union_rdd_10 = spark.sparkContext.union([task_rdd_1])
    task_rdd_10 = simulate_task(union_rdd_10, '10', 242)

    union_rdd_33 = spark.sparkContext.union([task_rdd_27, task_rdd_30, task_rdd_32])
    task_rdd_33 = simulate_task(union_rdd_33, '33', 3)

    union_rdd_28 = spark.sparkContext.union([task_rdd_54])
    task_rdd_28 = simulate_task(union_rdd_28, '28', 1)

    union_rdd_18 = spark.sparkContext.union([task_rdd_17])
    task_rdd_18 = simulate_task(union_rdd_18, '18', 376)

    union_rdd_22 = spark.sparkContext.union([task_rdd_21])
    task_rdd_22 = simulate_task(union_rdd_22, '22', 18)

    union_rdd_73 = spark.sparkContext.union([task_rdd_72])
    task_rdd_73 = simulate_task(union_rdd_73, '73', 74)

    union_rdd_31 = spark.sparkContext.union([task_rdd_27])
    task_rdd_31 = simulate_task(union_rdd_31, '31', 28)

    union_rdd_45 = spark.sparkContext.union([task_rdd_3, task_rdd_4])
    task_rdd_45 = simulate_task(union_rdd_45, '45', 17)

    union_rdd_14 = spark.sparkContext.union([task_rdd_10])
    task_rdd_14 = simulate_task(union_rdd_14, '14', 338)

    union_rdd_34 = spark.sparkContext.union([task_rdd_32, task_rdd_33])
    task_rdd_34 = simulate_task(union_rdd_34, '34', 1)

    union_rdd_19 = spark.sparkContext.union([task_rdd_12, task_rdd_18])
    task_rdd_19 = simulate_task(union_rdd_19, '19', 32)

    union_rdd_26 = spark.sparkContext.union([task_rdd_22, task_rdd_23])
    task_rdd_26 = simulate_task(union_rdd_26, '26', 5)

    union_rdd_69 = spark.sparkContext.union([task_rdd_31, task_rdd_36, task_rdd_47])
    task_rdd_69 = simulate_task(union_rdd_69, '69', 690)

    union_rdd_46 = spark.sparkContext.union([task_rdd_11, task_rdd_45])
    task_rdd_46 = simulate_task(union_rdd_46, '46', 11)

    union_rdd_15 = spark.sparkContext.union([task_rdd_11, task_rdd_12, task_rdd_14])
    task_rdd_15 = simulate_task(union_rdd_15, '15', 625)

    union_rdd_35 = spark.sparkContext.union([task_rdd_11, task_rdd_12, task_rdd_34])
    task_rdd_35 = simulate_task(union_rdd_35, '35', 141)

    union_rdd_70 = spark.sparkContext.union([task_rdd_69])
    task_rdd_70 = simulate_task(union_rdd_70, '70', 437)

    union_rdd_13 = spark.sparkContext.union([task_rdd_12, task_rdd_46])
    task_rdd_13 = simulate_task(union_rdd_13, '13', 18)

    union_rdd_2 = spark.sparkContext.union([task_rdd_12, task_rdd_15])
    task_rdd_2 = simulate_task(union_rdd_2, '2', 121)

    union_rdd_71 = spark.sparkContext.union([task_rdd_70])
    task_rdd_71 = simulate_task(union_rdd_71, '71', 22)

    union_rdd_9 = spark.sparkContext.union([])
    task_rdd_9 = simulate_task(union_rdd_9, '9', 42)

    union_rdd_50 = spark.sparkContext.union([task_rdd_44, task_rdd_47, task_rdd_48])
    task_rdd_50 = simulate_task(union_rdd_50, '50', 11)

    union_rdd_20 = spark.sparkContext.union([task_rdd_50])
    task_rdd_20 = simulate_task(union_rdd_20, '20', 13)

    union_rdd_38 = spark.sparkContext.union([task_rdd_16, task_rdd_17, task_rdd_20, task_rdd_23, task_rdd_26, task_rdd_28, task_rdd_30, task_rdd_37])
    task_rdd_38 = simulate_task(union_rdd_38, '38', 2)

    union_rdd_24 = spark.sparkContext.union([task_rdd_20])
    task_rdd_24 = simulate_task(union_rdd_24, '24', 16)

    union_rdd_40 = spark.sparkContext.union([task_rdd_38, task_rdd_39])
    task_rdd_40 = simulate_task(union_rdd_40, '40', 19)

    union_rdd_25 = spark.sparkContext.union([task_rdd_24])
    task_rdd_25 = simulate_task(union_rdd_25, '25', 30)

    union_rdd_41 = spark.sparkContext.union([task_rdd_38, task_rdd_40])
    task_rdd_41 = simulate_task(union_rdd_41, '41', 215)

    union_rdd_43 = spark.sparkContext.union([task_rdd_41, task_rdd_42])
    task_rdd_43 = simulate_task(union_rdd_43, '43', 62)

    combined_rdd = spark.sparkContext.union([task_rdd_3, task_rdd_6, task_rdd_8])
    task_rdd = simulate_task(combined_rdd, 'J7_3_6_8', 1)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
