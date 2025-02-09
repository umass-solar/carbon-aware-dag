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
    task_rdd_2 = simulate_task(base_rdd, '2', 18)
    task_rdd_12 = simulate_task(base_rdd, '12', 14)
    task_rdd_11 = simulate_task(base_rdd, '11', 7)
    task_rdd_4 = simulate_task(base_rdd, '4', 31)
    task_rdd_1 = simulate_task(base_rdd, '1', 10)
    task_rdd_17 = simulate_task(base_rdd, '17', 42)
    task_rdd_9 = simulate_task(base_rdd, '9', 10)
    task_rdd_8 = simulate_task(base_rdd, '8', 2)
    task_rdd_3 = simulate_task(base_rdd, '3', 10)
    task_rdd_92 = simulate_task(base_rdd, '92', 5)
    task_rdd_44 = simulate_task(base_rdd, '44', 2)
    task_rdd_42 = simulate_task(base_rdd, '42', 4)
    task_rdd_131 = simulate_task(base_rdd, '131', 4)
    task_rdd_7 = simulate_task(base_rdd, '7', 10)
    task_rdd_61 = simulate_task(base_rdd, '61', 162)
    task_rdd_35 = simulate_task(base_rdd, '35', 17)
    task_rdd_23 = simulate_task(base_rdd, '23', 65)
    task_rdd_22 = simulate_task(base_rdd, '22', 34)
    task_rdd_20 = simulate_task(base_rdd, '20', 15)
    task_rdd_19 = simulate_task(base_rdd, '19', 15)
    task_rdd_18 = simulate_task(base_rdd, '18', 41)
    task_rdd_38 = simulate_task(base_rdd, '38', 99)
    task_rdd_5 = simulate_task(base_rdd, '5', 7)
    task_rdd_16 = simulate_task(base_rdd, '16', 17)
    task_rdd_40 = simulate_task(base_rdd, '40', 182)
    task_rdd_15 = simulate_task(base_rdd, '15', 36)
    task_rdd_32 = simulate_task(base_rdd, '32', 25)
    task_rdd_29 = simulate_task(base_rdd, '29', 2)
    task_rdd_116 = simulate_task(base_rdd, '116', 4)
    task_rdd_49 = simulate_task(base_rdd, '49', 10)
    task_rdd_48 = simulate_task(base_rdd, '48', 25)
    task_rdd_46 = simulate_task(base_rdd, '46', 5)
    task_rdd_47 = simulate_task(base_rdd, '47', 2)
    task_rdd_60 = simulate_task(base_rdd, '60', 5)
    task_rdd_41 = simulate_task(base_rdd, '41', 109)
    task_rdd_50 = simulate_task(base_rdd, '50', 11)
    task_rdd_76 = simulate_task(base_rdd, '76', 35)
    task_rdd_72 = simulate_task(base_rdd, '72', 4)
    task_rdd_57 = simulate_task(base_rdd, '57', 19)

    union_rdd_28 = spark.sparkContext.union([task_rdd_1, task_rdd_2, task_rdd_3, task_rdd_4, task_rdd_5, task_rdd_6, task_rdd_7, task_rdd_8, task_rdd_9, task_rdd_11, task_rdd_12, task_rdd_15, task_rdd_16, task_rdd_17, task_rdd_18, task_rdd_19, task_rdd_2])
    task_rdd_28 = simulate_task(union_rdd_28, '28', 265)

    union_rdd_26 = spark.sparkContext.union([task_rdd_4, task_rdd_6])
    task_rdd_26 = simulate_task(union_rdd_26, '26', 21)

    union_rdd_10 = spark.sparkContext.union([task_rdd_17])
    task_rdd_10 = simulate_task(union_rdd_10, '10', 2)

    union_rdd_94 = spark.sparkContext.union([task_rdd_19, task_rdd_44, task_rdd_92])
    task_rdd_94 = simulate_task(union_rdd_94, '94', 12)

    union_rdd_132 = spark.sparkContext.union([task_rdd_131])
    task_rdd_132 = simulate_task(union_rdd_132, '132', 1)

    union_rdd_39 = spark.sparkContext.union([task_rdd_7])
    task_rdd_39 = simulate_task(union_rdd_39, '39', 85)

    union_rdd_24 = spark.sparkContext.union([task_rdd_22, task_rdd_23])
    task_rdd_24 = simulate_task(union_rdd_24, '24', 3)

    union_rdd_21 = spark.sparkContext.union([task_rdd_5, task_rdd_20])
    task_rdd_21 = simulate_task(union_rdd_21, '21', 105)

    union_rdd_30 = spark.sparkContext.union([task_rdd_29])
    task_rdd_30 = simulate_task(union_rdd_30, '30', 308)

    union_rdd_120 = spark.sparkContext.union([task_rdd_116])
    task_rdd_120 = simulate_task(union_rdd_120, '120', 72)

    union_rdd_62 = spark.sparkContext.union([task_rdd_47])
    task_rdd_62 = simulate_task(union_rdd_62, '62', 75)

    union_rdd_51 = spark.sparkContext.union([task_rdd_50])
    task_rdd_51 = simulate_task(union_rdd_51, '51', 39)

    union_rdd_77 = spark.sparkContext.union([task_rdd_57, task_rdd_72, task_rdd_76])
    task_rdd_77 = simulate_task(union_rdd_77, '77', 3)

    union_rdd_33 = spark.sparkContext.union([task_rdd_7, task_rdd_28])
    task_rdd_33 = simulate_task(union_rdd_33, '33', 64)

    union_rdd_74 = spark.sparkContext.union([task_rdd_132])
    task_rdd_74 = simulate_task(union_rdd_74, '74', 4)

    union_rdd_34 = spark.sparkContext.union([task_rdd_23, task_rdd_24])
    task_rdd_34 = simulate_task(union_rdd_34, '34', 179)

    union_rdd_31 = spark.sparkContext.union([task_rdd_30])
    task_rdd_31 = simulate_task(union_rdd_31, '31', 76)

    union_rdd_73 = spark.sparkContext.union([task_rdd_120])
    task_rdd_73 = simulate_task(union_rdd_73, '73', 4)

    union_rdd_63 = spark.sparkContext.union([task_rdd_60, task_rdd_62])
    task_rdd_63 = simulate_task(union_rdd_63, '63', 4)

    union_rdd_78 = spark.sparkContext.union([task_rdd_77])
    task_rdd_78 = simulate_task(union_rdd_78, '78', 10)

    union_rdd_52 = spark.sparkContext.union([task_rdd_33])
    task_rdd_52 = simulate_task(union_rdd_52, '52', 640)

    union_rdd_58 = spark.sparkContext.union([task_rdd_73, task_rdd_94])
    task_rdd_58 = simulate_task(union_rdd_58, '58', 13)

    union_rdd_36 = spark.sparkContext.union([task_rdd_22, task_rdd_34, task_rdd_35])
    task_rdd_36 = simulate_task(union_rdd_36, '36', 151)

    union_rdd_43 = spark.sparkContext.union([task_rdd_78])
    task_rdd_43 = simulate_task(union_rdd_43, '43', 2)

    union_rdd_25 = spark.sparkContext.union([task_rdd_63])
    task_rdd_25 = simulate_task(union_rdd_25, '25', 6)

    union_rdd_53 = spark.sparkContext.union([task_rdd_32, task_rdd_48, task_rdd_49, task_rdd_52])
    task_rdd_53 = simulate_task(union_rdd_53, '53', 9)

    union_rdd_59 = spark.sparkContext.union([task_rdd_58])
    task_rdd_59 = simulate_task(union_rdd_59, '59', 32)

    union_rdd_37 = spark.sparkContext.union([task_rdd_20, task_rdd_32, task_rdd_36])
    task_rdd_37 = simulate_task(union_rdd_37, '37', 19)

    union_rdd_27 = spark.sparkContext.union([task_rdd_25])
    task_rdd_27 = simulate_task(union_rdd_27, '27', 21)

    union_rdd_54 = spark.sparkContext.union([task_rdd_44, task_rdd_46, task_rdd_48, task_rdd_51, task_rdd_53])
    task_rdd_54 = simulate_task(union_rdd_54, '54', 17)

    union_rdd_45 = spark.sparkContext.union([task_rdd_37, task_rdd_39, task_rdd_42])
    task_rdd_45 = simulate_task(union_rdd_45, '45', 221)

    union_rdd_55 = spark.sparkContext.union([task_rdd_50, task_rdd_54])
    task_rdd_55 = simulate_task(union_rdd_55, '55', 8)

    union_rdd_56 = spark.sparkContext.union([task_rdd_42, task_rdd_43, task_rdd_55])
    task_rdd_56 = simulate_task(union_rdd_56, '56', 38)

    union_rdd_14 = spark.sparkContext.union([task_rdd_56])
    task_rdd_14 = simulate_task(union_rdd_14, '14', 6)

    union_rdd_13 = spark.sparkContext.union([task_rdd_14])
    task_rdd_13 = simulate_task(union_rdd_13, '13', 2)

    combined_rdd = spark.sparkContext.union([task_rdd_7, task_rdd_13])
    task_rdd = simulate_task(combined_rdd, 'J6_7_13', 33)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
