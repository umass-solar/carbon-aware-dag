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

    task_rdd_33 = simulate_task(base_rdd, '33', 4)
    task_rdd_7 = simulate_task(base_rdd, '7', 37)
    task_rdd_11 = simulate_task(base_rdd, '11', 27)
    task_rdd_6 = simulate_task(base_rdd, '6', 11)
    task_rdd_5 = simulate_task(base_rdd, '5', 19)
    task_rdd_4 = simulate_task(base_rdd, '4', 4)
    task_rdd_27 = simulate_task(base_rdd, '27', 38)
    task_rdd_3 = simulate_task(base_rdd, '3', 172)
    task_rdd_66 = simulate_task(base_rdd, '66', 141)
    task_rdd_55 = simulate_task(base_rdd, '55', 32)
    task_rdd_1 = simulate_task(base_rdd, '1', 12)
    task_rdd_46 = simulate_task(base_rdd, '46', 6)
    task_rdd_51 = simulate_task(base_rdd, '51', 15)
    task_rdd_38 = simulate_task(base_rdd, '38', 77)
    task_rdd_132 = simulate_task(base_rdd, '132', 4)
    task_rdd_10 = simulate_task(base_rdd, '10', 106)
    task_rdd_18 = simulate_task(base_rdd, '18', 26)
    task_rdd_2 = simulate_task(base_rdd, '2', 5)
    task_rdd_20 = simulate_task(base_rdd, '20', 11)
    task_rdd_97 = simulate_task(base_rdd, '97', 11)
    task_rdd_83 = simulate_task(base_rdd, '83', 7)

    union_rdd_35 = spark.sparkContext.union([])
    task_rdd_35 = simulate_task(union_rdd_35, '35', 2)

    union_rdd_68 = spark.sparkContext.union([task_rdd_66])
    task_rdd_68 = simulate_task(union_rdd_68, '68', 18)

    union_rdd_44 = spark.sparkContext.union([task_rdd_46])
    task_rdd_44 = simulate_task(union_rdd_44, '44', 5)

    union_rdd_40 = spark.sparkContext.union([task_rdd_38])
    task_rdd_40 = simulate_task(union_rdd_40, '40', 19)

    union_rdd_19 = spark.sparkContext.union([task_rdd_18])
    task_rdd_19 = simulate_task(union_rdd_19, '19', 4)

    union_rdd_99 = spark.sparkContext.union([])
    task_rdd_99 = simulate_task(union_rdd_99, '99', 3)

    union_rdd_36 = spark.sparkContext.union([task_rdd_5, task_rdd_10, task_rdd_20, task_rdd_35])
    task_rdd_36 = simulate_task(union_rdd_36, '36', 14)

    union_rdd_69 = spark.sparkContext.union([task_rdd_66, task_rdd_68])
    task_rdd_69 = simulate_task(union_rdd_69, '69', 6)

    union_rdd_73 = spark.sparkContext.union([task_rdd_44])
    task_rdd_73 = simulate_task(union_rdd_73, '73', 43)

    union_rdd_41 = spark.sparkContext.union([task_rdd_40])
    task_rdd_41 = simulate_task(union_rdd_41, '41', 18)

    union_rdd_100 = spark.sparkContext.union([task_rdd_99])
    task_rdd_100 = simulate_task(union_rdd_100, '100', 97)

    union_rdd_13 = spark.sparkContext.union([task_rdd_100])
    task_rdd_13 = simulate_task(union_rdd_13, '13', 32)

    union_rdd_29 = spark.sparkContext.union([task_rdd_36])
    task_rdd_29 = simulate_task(union_rdd_29, '29', 77)

    union_rdd_58 = spark.sparkContext.union([task_rdd_73])
    task_rdd_58 = simulate_task(union_rdd_58, '58', 6)

    union_rdd_25 = spark.sparkContext.union([task_rdd_13])
    task_rdd_25 = simulate_task(union_rdd_25, '25', 176)

    union_rdd_30 = spark.sparkContext.union([task_rdd_10, task_rdd_11, task_rdd_29])
    task_rdd_30 = simulate_task(union_rdd_30, '30', 98)

    union_rdd_87 = spark.sparkContext.union([task_rdd_58])
    task_rdd_87 = simulate_task(union_rdd_87, '87', 60)

    union_rdd_14 = spark.sparkContext.union([task_rdd_13, task_rdd_25])
    task_rdd_14 = simulate_task(union_rdd_14, '14', 74)

    union_rdd_31 = spark.sparkContext.union([task_rdd_30])
    task_rdd_31 = simulate_task(union_rdd_31, '31', 46)

    union_rdd_88 = spark.sparkContext.union([task_rdd_41, task_rdd_51, task_rdd_87])
    task_rdd_88 = simulate_task(union_rdd_88, '88', 21)

    union_rdd_15 = spark.sparkContext.union([task_rdd_4, task_rdd_14])
    task_rdd_15 = simulate_task(union_rdd_15, '15', 10)

    union_rdd_32 = spark.sparkContext.union([task_rdd_3, task_rdd_4, task_rdd_10, task_rdd_11, task_rdd_31])
    task_rdd_32 = simulate_task(union_rdd_32, '32', 1033)

    union_rdd_59 = spark.sparkContext.union([task_rdd_88])
    task_rdd_59 = simulate_task(union_rdd_59, '59', 16)

    union_rdd_16 = spark.sparkContext.union([task_rdd_6, task_rdd_10, task_rdd_15])
    task_rdd_16 = simulate_task(union_rdd_16, '16', 20)

    union_rdd_8 = spark.sparkContext.union([task_rdd_32, task_rdd_33])
    task_rdd_8 = simulate_task(union_rdd_8, '8', 29)

    union_rdd_71 = spark.sparkContext.union([task_rdd_16])
    task_rdd_71 = simulate_task(union_rdd_71, '71', 58)

    union_rdd_12 = spark.sparkContext.union([task_rdd_7, task_rdd_8])
    task_rdd_12 = simulate_task(union_rdd_12, '12', 4)

    union_rdd_72 = spark.sparkContext.union([task_rdd_27, task_rdd_71])
    task_rdd_72 = simulate_task(union_rdd_72, '72', 6)

    combined_rdd = spark.sparkContext.union([task_rdd_72])
    task_rdd = simulate_task(combined_rdd, 'R37_72', 1)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
