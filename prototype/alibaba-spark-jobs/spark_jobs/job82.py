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

    task_rdd_15 = simulate_task(base_rdd, '15', 25)
    task_rdd_12 = simulate_task(base_rdd, '12', 14)
    task_rdd_11 = simulate_task(base_rdd, '11', 2)
    task_rdd_10 = simulate_task(base_rdd, '10', 15)
    task_rdd_9 = simulate_task(base_rdd, '9', 9)
    task_rdd_8 = simulate_task(base_rdd, '8', 6)
    task_rdd_7 = simulate_task(base_rdd, '7', 111)
    task_rdd_86 = simulate_task(base_rdd, '86', 2)
    task_rdd_82 = simulate_task(base_rdd, '82', 72)
    task_rdd_1 = simulate_task(base_rdd, '1', 5)
    task_rdd_6 = simulate_task(base_rdd, '6', 11)
    task_rdd_5 = simulate_task(base_rdd, '5', 5)
    task_rdd_23 = simulate_task(base_rdd, '23', 50)
    task_rdd_19 = simulate_task(base_rdd, '19', 32)
    task_rdd_2 = simulate_task(base_rdd, '2', 121)
    task_rdd_4 = simulate_task(base_rdd, '4', 1)
    task_rdd_3 = simulate_task(base_rdd, '3', 19)
    task_rdd_93 = simulate_task(base_rdd, '93', 4)
    task_rdd_55 = simulate_task(base_rdd, '55', 29)
    task_rdd_51 = simulate_task(base_rdd, '51', 3)
    task_rdd_50 = simulate_task(base_rdd, '50', 47)
    task_rdd_44 = simulate_task(base_rdd, '44', 4)
    task_rdd_42 = simulate_task(base_rdd, '42', 50)
    task_rdd_132 = simulate_task(base_rdd, '132', 4)
    task_rdd_89 = simulate_task(base_rdd, '89', 4)
    task_rdd_36 = simulate_task(base_rdd, '36', 26)
    task_rdd_63 = simulate_task(base_rdd, '63', 4)

    union_rdd_13 = spark.sparkContext.union([task_rdd_8, task_rdd_9, task_rdd_11, task_rdd_12])
    task_rdd_13 = simulate_task(union_rdd_13, '13', 9)

    union_rdd_16 = spark.sparkContext.union([task_rdd_3, task_rdd_4, task_rdd_5, task_rdd_6, task_rdd_7, task_rdd_8, task_rdd_9, task_rdd_10, task_rdd_15])
    task_rdd_16 = simulate_task(union_rdd_16, '16', 160)

    union_rdd_28 = spark.sparkContext.union([task_rdd_82])
    task_rdd_28 = simulate_task(union_rdd_28, '28', 16)

    union_rdd_74 = spark.sparkContext.union([task_rdd_1, task_rdd_2, task_rdd_3, task_rdd_4, task_rdd_5, task_rdd_6, task_rdd_1])
    task_rdd_74 = simulate_task(union_rdd_74, '74', 40)

    union_rdd_20 = spark.sparkContext.union([task_rdd_19])
    task_rdd_20 = simulate_task(union_rdd_20, '20', 22)

    union_rdd_100 = spark.sparkContext.union([task_rdd_93])
    task_rdd_100 = simulate_task(union_rdd_100, '100', 13)

    union_rdd_58 = spark.sparkContext.union([task_rdd_51, task_rdd_55])
    task_rdd_58 = simulate_task(union_rdd_58, '58', 186)

    union_rdd_47 = spark.sparkContext.union([task_rdd_50])
    task_rdd_47 = simulate_task(union_rdd_47, '47', 406)

    union_rdd_34 = spark.sparkContext.union([task_rdd_44])
    task_rdd_34 = simulate_task(union_rdd_34, '34', 2)

    union_rdd_119 = spark.sparkContext.union([task_rdd_132])
    task_rdd_119 = simulate_task(union_rdd_119, '119', 4)

    union_rdd_14 = spark.sparkContext.union([task_rdd_4, task_rdd_5, task_rdd_12, task_rdd_13])
    task_rdd_14 = simulate_task(union_rdd_14, '14', 125)

    union_rdd_17 = spark.sparkContext.union([task_rdd_15, task_rdd_16])
    task_rdd_17 = simulate_task(union_rdd_17, '17', 20)

    union_rdd_79 = spark.sparkContext.union([task_rdd_100])
    task_rdd_79 = simulate_task(union_rdd_79, '79', 5)

    union_rdd_87 = spark.sparkContext.union([task_rdd_28, task_rdd_86])
    task_rdd_87 = simulate_task(union_rdd_87, '87', 5)

    union_rdd_21 = spark.sparkContext.union([task_rdd_20])
    task_rdd_21 = simulate_task(union_rdd_21, '21', 28)

    union_rdd_35 = spark.sparkContext.union([task_rdd_34])
    task_rdd_35 = simulate_task(union_rdd_35, '35', 25)

    union_rdd_120 = spark.sparkContext.union([task_rdd_89, task_rdd_119])
    task_rdd_120 = simulate_task(union_rdd_120, '120', 275)

    union_rdd_39 = spark.sparkContext.union([task_rdd_7, task_rdd_8, task_rdd_17])
    task_rdd_39 = simulate_task(union_rdd_39, '39', 514)

    union_rdd_80 = spark.sparkContext.union([task_rdd_74, task_rdd_79])
    task_rdd_80 = simulate_task(union_rdd_80, '80', 340)

    union_rdd_24 = spark.sparkContext.union([task_rdd_21, task_rdd_23])
    task_rdd_24 = simulate_task(union_rdd_24, '24', 267)

    union_rdd_48 = spark.sparkContext.union([task_rdd_35, task_rdd_47])
    task_rdd_48 = simulate_task(union_rdd_48, '48', 28)

    union_rdd_69 = spark.sparkContext.union([task_rdd_120])
    task_rdd_69 = simulate_task(union_rdd_69, '69', 4)

    union_rdd_40 = spark.sparkContext.union([task_rdd_10, task_rdd_15, task_rdd_39])
    task_rdd_40 = simulate_task(union_rdd_40, '40', 8)

    union_rdd_81 = spark.sparkContext.union([task_rdd_79, task_rdd_80])
    task_rdd_81 = simulate_task(union_rdd_81, '81', 102)

    union_rdd_25 = spark.sparkContext.union([task_rdd_20, task_rdd_24])
    task_rdd_25 = simulate_task(union_rdd_25, '25', 122)

    union_rdd_45 = spark.sparkContext.union([task_rdd_44, task_rdd_48])
    task_rdd_45 = simulate_task(union_rdd_45, '45', 54)

    union_rdd_52 = spark.sparkContext.union([task_rdd_40])
    task_rdd_52 = simulate_task(union_rdd_52, '52', 4)

    union_rdd_59 = spark.sparkContext.union([task_rdd_58, task_rdd_81])
    task_rdd_59 = simulate_task(union_rdd_59, '59', 3)

    union_rdd_38 = spark.sparkContext.union([task_rdd_51, task_rdd_52])
    task_rdd_38 = simulate_task(union_rdd_38, '38', 2)

    union_rdd_60 = spark.sparkContext.union([task_rdd_44, task_rdd_45, task_rdd_59])
    task_rdd_60 = simulate_task(union_rdd_60, '60', 24)

    union_rdd_56 = spark.sparkContext.union([task_rdd_60])
    task_rdd_56 = simulate_task(union_rdd_56, '56', 26)

    union_rdd_43 = spark.sparkContext.union([task_rdd_42, task_rdd_56])
    task_rdd_43 = simulate_task(union_rdd_43, '43', 46)

    union_rdd_18 = spark.sparkContext.union([task_rdd_43])
    task_rdd_18 = simulate_task(union_rdd_18, '18', 75)

    combined_rdd = spark.sparkContext.union([task_rdd_2, task_rdd_4, task_rdd_6, task_rdd_8, task_rdd_10, task_rdd_14])
    task_rdd = simulate_task(combined_rdd, 'R15_2_4_6_8_10_14', 18)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
