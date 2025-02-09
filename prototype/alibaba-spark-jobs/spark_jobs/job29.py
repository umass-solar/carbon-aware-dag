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

    task_rdd_2 = simulate_task(base_rdd, '2', 9)
    task_rdd_9 = simulate_task(base_rdd, '9', 8)
    task_rdd_8 = simulate_task(base_rdd, '8', 12)
    task_rdd_7 = simulate_task(base_rdd, '7', 6)
    task_rdd_4 = simulate_task(base_rdd, '4', 76)
    task_rdd_1 = simulate_task(base_rdd, '1', 4)
    task_rdd_34 = simulate_task(base_rdd, '34', 2)
    task_rdd_31 = simulate_task(base_rdd, '31', 13)
    task_rdd_16 = simulate_task(base_rdd, '16', 133)
    task_rdd_11 = simulate_task(base_rdd, '11', 5)
    task_rdd_30 = simulate_task(base_rdd, '30', 5)
    task_rdd_19 = simulate_task(base_rdd, '19', 38)
    task_rdd_98 = simulate_task(base_rdd, '98', 4)
    task_rdd_75 = simulate_task(base_rdd, '75', 502)
    task_rdd_70 = simulate_task(base_rdd, '70', 172)
    task_rdd_56 = simulate_task(base_rdd, '56', 6)
    task_rdd_35 = simulate_task(base_rdd, '35', 4)
    task_rdd_129 = simulate_task(base_rdd, '129', 4)
    task_rdd_20 = simulate_task(base_rdd, '20', 47)
    task_rdd_62 = simulate_task(base_rdd, '62', 4)
    task_rdd_66 = simulate_task(base_rdd, '66', 32)

    union_rdd_3 = spark.sparkContext.union([task_rdd_2])
    task_rdd_3 = simulate_task(union_rdd_3, '3', 6)

    union_rdd_10 = spark.sparkContext.union([task_rdd_7])
    task_rdd_10 = simulate_task(union_rdd_10, '10', 11)

    union_rdd_32 = spark.sparkContext.union([task_rdd_16, task_rdd_19, task_rdd_31])
    task_rdd_32 = simulate_task(union_rdd_32, '32', 312)

    union_rdd_17 = spark.sparkContext.union([task_rdd_11, task_rdd_16])
    task_rdd_17 = simulate_task(union_rdd_17, '17', 27)

    union_rdd_21 = spark.sparkContext.union([task_rdd_19])
    task_rdd_21 = simulate_task(union_rdd_21, '21', 187)

    union_rdd_99 = spark.sparkContext.union([task_rdd_98])
    task_rdd_99 = simulate_task(union_rdd_99, '99', 14)

    union_rdd_71 = spark.sparkContext.union([task_rdd_70, task_rdd_75])
    task_rdd_71 = simulate_task(union_rdd_71, '71', 1)

    union_rdd_57 = spark.sparkContext.union([task_rdd_56])
    task_rdd_57 = simulate_task(union_rdd_57, '57', 106)

    union_rdd_36 = spark.sparkContext.union([task_rdd_35])
    task_rdd_36 = simulate_task(union_rdd_36, '36', 189)

    union_rdd_130 = spark.sparkContext.union([task_rdd_129])
    task_rdd_130 = simulate_task(union_rdd_130, '130', 12)

    union_rdd_63 = spark.sparkContext.union([task_rdd_62])
    task_rdd_63 = simulate_task(union_rdd_63, '63', 20)

    union_rdd_22 = spark.sparkContext.union([task_rdd_66])
    task_rdd_22 = simulate_task(union_rdd_22, '22', 2)

    union_rdd_14 = spark.sparkContext.union([task_rdd_3])
    task_rdd_14 = simulate_task(union_rdd_14, '14', 29)

    union_rdd_15 = spark.sparkContext.union([task_rdd_10])
    task_rdd_15 = simulate_task(union_rdd_15, '15', 241)

    union_rdd_33 = spark.sparkContext.union([task_rdd_63])
    task_rdd_33 = simulate_task(union_rdd_33, '33', 97)

    union_rdd_18 = spark.sparkContext.union([task_rdd_11, task_rdd_17])
    task_rdd_18 = simulate_task(union_rdd_18, '18', 40)

    union_rdd_100 = spark.sparkContext.union([task_rdd_99])
    task_rdd_100 = simulate_task(union_rdd_100, '100', 642)

    union_rdd_72 = spark.sparkContext.union([task_rdd_71])
    task_rdd_72 = simulate_task(union_rdd_72, '72', 22)

    union_rdd_58 = spark.sparkContext.union([task_rdd_57])
    task_rdd_58 = simulate_task(union_rdd_58, '58', 31)

    union_rdd_131 = spark.sparkContext.union([task_rdd_130])
    task_rdd_131 = simulate_task(union_rdd_131, '131', 3)

    union_rdd_23 = spark.sparkContext.union([task_rdd_22])
    task_rdd_23 = simulate_task(union_rdd_23, '23', 20)

    union_rdd_12 = spark.sparkContext.union([task_rdd_9, task_rdd_11, task_rdd_14])
    task_rdd_12 = simulate_task(union_rdd_12, '12', 3)

    union_rdd_41 = spark.sparkContext.union([task_rdd_18])
    task_rdd_41 = simulate_task(union_rdd_41, '41', 11)

    union_rdd_79 = spark.sparkContext.union([task_rdd_100])
    task_rdd_79 = simulate_task(union_rdd_79, '79', 14)

    union_rdd_73 = spark.sparkContext.union([task_rdd_72])
    task_rdd_73 = simulate_task(union_rdd_73, '73', 32)

    union_rdd_37 = spark.sparkContext.union([task_rdd_20, task_rdd_23, task_rdd_36])
    task_rdd_37 = simulate_task(union_rdd_37, '37', 48)

    union_rdd_132 = spark.sparkContext.union([task_rdd_131])
    task_rdd_132 = simulate_task(union_rdd_132, '132', 1)

    union_rdd_13 = spark.sparkContext.union([task_rdd_1, task_rdd_7, task_rdd_8, task_rdd_11, task_rdd_12])
    task_rdd_13 = simulate_task(union_rdd_13, '13', 29)

    union_rdd_42 = spark.sparkContext.union([task_rdd_41])
    task_rdd_42 = simulate_task(union_rdd_42, '42', 1223)

    union_rdd_74 = spark.sparkContext.union([task_rdd_73])
    task_rdd_74 = simulate_task(union_rdd_74, '74', 486)

    union_rdd_38 = spark.sparkContext.union([task_rdd_37])
    task_rdd_38 = simulate_task(union_rdd_38, '38', 45)

    union_rdd_47 = spark.sparkContext.union([task_rdd_13])
    task_rdd_47 = simulate_task(union_rdd_47, '47', 164)

    union_rdd_80 = spark.sparkContext.union([task_rdd_74, task_rdd_79])
    task_rdd_80 = simulate_task(union_rdd_80, '80', 340)

    union_rdd_39 = spark.sparkContext.union([task_rdd_38])
    task_rdd_39 = simulate_task(union_rdd_39, '39', 126)

    union_rdd_81 = spark.sparkContext.union([task_rdd_80])
    task_rdd_81 = simulate_task(union_rdd_81, '81', 2)

    union_rdd_59 = spark.sparkContext.union([task_rdd_58, task_rdd_81])
    task_rdd_59 = simulate_task(union_rdd_59, '59', 3)

    union_rdd_44 = spark.sparkContext.union([task_rdd_59])
    task_rdd_44 = simulate_task(union_rdd_44, '44', 9)

    union_rdd_45 = spark.sparkContext.union([task_rdd_44])
    task_rdd_45 = simulate_task(union_rdd_45, '45', 77)

    union_rdd_48 = spark.sparkContext.union([task_rdd_45, task_rdd_47])
    task_rdd_48 = simulate_task(union_rdd_48, '48', 3)

    union_rdd_40 = spark.sparkContext.union([task_rdd_30, task_rdd_31, task_rdd_33, task_rdd_34, task_rdd_35, task_rdd_39, task_rdd_48])
    task_rdd_40 = simulate_task(union_rdd_40, '40', 278)

    union_rdd_24 = spark.sparkContext.union([task_rdd_40])
    task_rdd_24 = simulate_task(union_rdd_24, '24', 2)

    union_rdd_25 = spark.sparkContext.union([task_rdd_23, task_rdd_24])
    task_rdd_25 = simulate_task(union_rdd_25, '25', 28)

    union_rdd_28 = spark.sparkContext.union([task_rdd_25])
    task_rdd_28 = simulate_task(union_rdd_28, '28', 3)

    union_rdd_29 = spark.sparkContext.union([task_rdd_1, task_rdd_2, task_rdd_28])
    task_rdd_29 = simulate_task(union_rdd_29, '29', 84)

    union_rdd_27 = spark.sparkContext.union([task_rdd_29])
    task_rdd_27 = simulate_task(union_rdd_27, '27', 192)

    union_rdd_6 = spark.sparkContext.union([task_rdd_9, task_rdd_27])
    task_rdd_6 = simulate_task(union_rdd_6, '6', 137)

    union_rdd_5 = spark.sparkContext.union([task_rdd_2, task_rdd_6])
    task_rdd_5 = simulate_task(union_rdd_5, '5', 58)

    union_rdd_120 = spark.sparkContext.union([task_rdd_5])
    task_rdd_120 = simulate_task(union_rdd_120, '120', 36)

    combined_rdd = spark.sparkContext.union([task_rdd_120])
    task_rdd = simulate_task(combined_rdd, 'R121_120', 2)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
