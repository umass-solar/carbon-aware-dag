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

    task_rdd_10 = simulate_task(base_rdd, '10', 4)
    task_rdd_8 = simulate_task(base_rdd, '8', 16)
    task_rdd_18 = simulate_task(base_rdd, '18', 3)
    task_rdd_6 = simulate_task(base_rdd, '6', 1)
    task_rdd_5 = simulate_task(base_rdd, '5', 11)
    task_rdd_12 = simulate_task(base_rdd, '12', 7)
    task_rdd_2 = simulate_task(base_rdd, '2', 14)
    task_rdd_14 = simulate_task(base_rdd, '14', 20)
    task_rdd_37 = simulate_task(base_rdd, '37', 4)
    task_rdd_40 = simulate_task(base_rdd, '40', 25)
    task_rdd_32 = simulate_task(base_rdd, '32', 103)
    task_rdd_30 = simulate_task(base_rdd, '30', 18)
    task_rdd_50 = simulate_task(base_rdd, '50', 24)
    task_rdd_55 = simulate_task(base_rdd, '55', 7)
    task_rdd_1 = simulate_task(base_rdd, '1', 24)
    task_rdd_42 = simulate_task(base_rdd, '42', 4)
    task_rdd_24 = simulate_task(base_rdd, '24', 38)
    task_rdd_4 = simulate_task(base_rdd, '4', 3)
    task_rdd_21 = simulate_task(base_rdd, '21', 29)
    task_rdd_38 = simulate_task(base_rdd, '38', 10)
    task_rdd_51 = simulate_task(base_rdd, '51', 5)
    task_rdd_67 = simulate_task(base_rdd, '67', 7)
    task_rdd_44 = simulate_task(base_rdd, '44', 10)
    task_rdd_59 = simulate_task(base_rdd, '59', 72)
    task_rdd_52 = simulate_task(base_rdd, '52', 67)
    task_rdd_48 = simulate_task(base_rdd, '48', 72)
    task_rdd_60 = simulate_task(base_rdd, '60', 18)
    task_rdd_58 = simulate_task(base_rdd, '58', 115)

    union_rdd_11 = spark.sparkContext.union([task_rdd_5, task_rdd_10])
    task_rdd_11 = simulate_task(union_rdd_11, '11', 735)

    union_rdd_19 = spark.sparkContext.union([task_rdd_14, task_rdd_18])
    task_rdd_19 = simulate_task(union_rdd_19, '19', 207)

    union_rdd_15 = spark.sparkContext.union([task_rdd_4, task_rdd_6])
    task_rdd_15 = simulate_task(union_rdd_15, '15', 23)

    union_rdd_28 = spark.sparkContext.union([task_rdd_12])
    task_rdd_28 = simulate_task(union_rdd_28, '28', 17)

    union_rdd_20 = spark.sparkContext.union([task_rdd_2])
    task_rdd_20 = simulate_task(union_rdd_20, '20', 81)

    union_rdd_13 = spark.sparkContext.union([task_rdd_37])
    task_rdd_13 = simulate_task(union_rdd_13, '13', 2)

    union_rdd_33 = spark.sparkContext.union([task_rdd_32])
    task_rdd_33 = simulate_task(union_rdd_33, '33', 1)

    union_rdd_43 = spark.sparkContext.union([task_rdd_42])
    task_rdd_43 = simulate_task(union_rdd_43, '43', 395)

    union_rdd_23 = spark.sparkContext.union([task_rdd_4])
    task_rdd_23 = simulate_task(union_rdd_23, '23', 199)

    union_rdd_39 = spark.sparkContext.union([task_rdd_38])
    task_rdd_39 = simulate_task(union_rdd_39, '39', 13)

    union_rdd_61 = spark.sparkContext.union([task_rdd_60])
    task_rdd_61 = simulate_task(union_rdd_61, '61', 243)

    union_rdd_46 = spark.sparkContext.union([task_rdd_59])
    task_rdd_46 = simulate_task(union_rdd_46, '46', 42)

    union_rdd_72 = spark.sparkContext.union([task_rdd_67])
    task_rdd_72 = simulate_task(union_rdd_72, '72', 72)

    union_rdd_45 = spark.sparkContext.union([task_rdd_44])
    task_rdd_45 = simulate_task(union_rdd_45, '45', 2)

    union_rdd_49 = spark.sparkContext.union([task_rdd_48])
    task_rdd_49 = simulate_task(union_rdd_49, '49', 1)

    union_rdd_7 = spark.sparkContext.union([task_rdd_2, task_rdd_4, task_rdd_5, task_rdd_6, task_rdd_11])
    task_rdd_7 = simulate_task(union_rdd_7, '7', 12)

    union_rdd_29 = spark.sparkContext.union([task_rdd_14, task_rdd_28])
    task_rdd_29 = simulate_task(union_rdd_29, '29', 2)

    union_rdd_34 = spark.sparkContext.union([task_rdd_30, task_rdd_33])
    task_rdd_34 = simulate_task(union_rdd_34, '34', 767)

    union_rdd_31 = spark.sparkContext.union([task_rdd_39])
    task_rdd_31 = simulate_task(union_rdd_31, '31', 57)

    union_rdd_56 = spark.sparkContext.union([task_rdd_42, task_rdd_43, task_rdd_55])
    task_rdd_56 = simulate_task(union_rdd_56, '56', 38)

    union_rdd_47 = spark.sparkContext.union([task_rdd_46])
    task_rdd_47 = simulate_task(union_rdd_47, '47', 14)

    union_rdd_53 = spark.sparkContext.union([task_rdd_7])
    task_rdd_53 = simulate_task(union_rdd_53, '53', 6)

    union_rdd_35 = spark.sparkContext.union([task_rdd_31, task_rdd_32, task_rdd_34])
    task_rdd_35 = simulate_task(union_rdd_35, '35', 4)

    union_rdd_57 = spark.sparkContext.union([task_rdd_56])
    task_rdd_57 = simulate_task(union_rdd_57, '57', 6)

    union_rdd_62 = spark.sparkContext.union([task_rdd_46, task_rdd_47, task_rdd_61])
    task_rdd_62 = simulate_task(union_rdd_62, '62', 16)

    union_rdd_54 = spark.sparkContext.union([task_rdd_50, task_rdd_52, task_rdd_53])
    task_rdd_54 = simulate_task(union_rdd_54, '54', 1)

    union_rdd_41 = spark.sparkContext.union([task_rdd_21, task_rdd_24, task_rdd_30, task_rdd_35, task_rdd_40])
    task_rdd_41 = simulate_task(union_rdd_41, '41', 8)

    union_rdd_64 = spark.sparkContext.union([task_rdd_54])
    task_rdd_64 = simulate_task(union_rdd_64, '64', 7)

    union_rdd_100 = spark.sparkContext.union([task_rdd_41])
    task_rdd_100 = simulate_task(union_rdd_100, '100', 152)

    union_rdd_65 = spark.sparkContext.union([task_rdd_55, task_rdd_58, task_rdd_61, task_rdd_64])
    task_rdd_65 = simulate_task(union_rdd_65, '65', 11)

    union_rdd_73 = spark.sparkContext.union([task_rdd_100])
    task_rdd_73 = simulate_task(union_rdd_73, '73', 14)

    union_rdd_66 = spark.sparkContext.union([task_rdd_50, task_rdd_51, task_rdd_65])
    task_rdd_66 = simulate_task(union_rdd_66, '66', 21)

    union_rdd_25 = spark.sparkContext.union([task_rdd_62, task_rdd_73])
    task_rdd_25 = simulate_task(union_rdd_25, '25', 15)

    union_rdd_16 = spark.sparkContext.union([task_rdd_66])
    task_rdd_16 = simulate_task(union_rdd_16, '16', 5)

    union_rdd_26 = spark.sparkContext.union([task_rdd_25])
    task_rdd_26 = simulate_task(union_rdd_26, '26', 71)

    union_rdd_17 = spark.sparkContext.union([task_rdd_10, task_rdd_12, task_rdd_14, task_rdd_16])
    task_rdd_17 = simulate_task(union_rdd_17, '17', 13)

    union_rdd_27 = spark.sparkContext.union([task_rdd_26, task_rdd_29])
    task_rdd_27 = simulate_task(union_rdd_27, '27', 4)

    union_rdd_9 = spark.sparkContext.union([task_rdd_2])
    task_rdd_9 = simulate_task(union_rdd_9, '9', 20)

    union_rdd_22 = spark.sparkContext.union([])
    task_rdd_22 = simulate_task(union_rdd_22, '22', 10)

    combined_rdd = spark.sparkContext.union([task_rdd_2, task_rdd_8, task_rdd_9])
    task_rdd = simulate_task(combined_rdd, 'J3_2_8_9', 17)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
