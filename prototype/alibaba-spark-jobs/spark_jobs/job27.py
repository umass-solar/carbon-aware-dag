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

    task_rdd_15 = simulate_task(base_rdd, '15', 7)
    task_rdd_22 = simulate_task(base_rdd, '22', 11)
    task_rdd_21 = simulate_task(base_rdd, '21', 6)
    task_rdd_14 = simulate_task(base_rdd, '14', 3)
    task_rdd_11 = simulate_task(base_rdd, '11', 9)
    task_rdd_4 = simulate_task(base_rdd, '4', 125)
    task_rdd_6 = simulate_task(base_rdd, '6', 3)
    task_rdd_5 = simulate_task(base_rdd, '5', 3)
    task_rdd_39 = simulate_task(base_rdd, '39', 94)
    task_rdd_3 = simulate_task(base_rdd, '3', 5)
    task_rdd_8 = simulate_task(base_rdd, '8', 8)
    task_rdd_2 = simulate_task(base_rdd, '2', 2)
    task_rdd_10 = simulate_task(base_rdd, '10', 22)
    task_rdd_12 = simulate_task(base_rdd, '12', 5)
    task_rdd_17 = simulate_task(base_rdd, '17', 77)
    task_rdd_24 = simulate_task(base_rdd, '24', 78)
    task_rdd_128 = simulate_task(base_rdd, '128', 8)
    task_rdd_74 = simulate_task(base_rdd, '74', 9)
    task_rdd_57 = simulate_task(base_rdd, '57', 13)
    task_rdd_47 = simulate_task(base_rdd, '47', 2)
    task_rdd_56 = simulate_task(base_rdd, '56', 14)
    task_rdd_49 = simulate_task(base_rdd, '49', 4)
    task_rdd_32 = simulate_task(base_rdd, '32', 96)
    task_rdd_37 = simulate_task(base_rdd, '37', 145)
    task_rdd_33 = simulate_task(base_rdd, '33', 52)
    task_rdd_13 = simulate_task(base_rdd, '13', 56)
    task_rdd_61 = simulate_task(base_rdd, '61', 4)

    union_rdd_1 = spark.sparkContext.union([task_rdd_3, task_rdd_6])
    task_rdd_1 = simulate_task(union_rdd_1, '1', 65)

    union_rdd_28 = spark.sparkContext.union([task_rdd_17])
    task_rdd_28 = simulate_task(union_rdd_28, '28', 24)

    union_rdd_31 = spark.sparkContext.union([task_rdd_3])
    task_rdd_31 = simulate_task(union_rdd_31, '31', 744)

    union_rdd_16 = spark.sparkContext.union([task_rdd_10])
    task_rdd_16 = simulate_task(union_rdd_16, '16', 152)

    union_rdd_25 = spark.sparkContext.union([task_rdd_24])
    task_rdd_25 = simulate_task(union_rdd_25, '25', 5)

    union_rdd_129 = spark.sparkContext.union([task_rdd_128])
    task_rdd_129 = simulate_task(union_rdd_129, '129', 5)

    union_rdd_91 = spark.sparkContext.union([task_rdd_74])
    task_rdd_91 = simulate_task(union_rdd_91, '91', 82)

    union_rdd_58 = spark.sparkContext.union([task_rdd_47, task_rdd_57])
    task_rdd_58 = simulate_task(union_rdd_58, '58', 72)

    union_rdd_40 = spark.sparkContext.union([task_rdd_32])
    task_rdd_40 = simulate_task(union_rdd_40, '40', 68)

    union_rdd_51 = spark.sparkContext.union([task_rdd_49])
    task_rdd_51 = simulate_task(union_rdd_51, '51', 3)

    union_rdd_38 = spark.sparkContext.union([task_rdd_37])
    task_rdd_38 = simulate_task(union_rdd_38, '38', 17)

    union_rdd_34 = spark.sparkContext.union([task_rdd_33])
    task_rdd_34 = simulate_task(union_rdd_34, '34', 6)

    union_rdd_44 = spark.sparkContext.union([task_rdd_61])
    task_rdd_44 = simulate_task(union_rdd_44, '44', 1)

    union_rdd_42 = spark.sparkContext.union([task_rdd_31])
    task_rdd_42 = simulate_task(union_rdd_42, '42', 28)

    union_rdd_130 = spark.sparkContext.union([task_rdd_129])
    task_rdd_130 = simulate_task(union_rdd_130, '130', 1)

    union_rdd_93 = spark.sparkContext.union([task_rdd_91])
    task_rdd_93 = simulate_task(union_rdd_93, '93', 72)

    union_rdd_59 = spark.sparkContext.union([task_rdd_40, task_rdd_58])
    task_rdd_59 = simulate_task(union_rdd_59, '59', 28)

    union_rdd_54 = spark.sparkContext.union([task_rdd_51])
    task_rdd_54 = simulate_task(union_rdd_54, '54', 7)

    union_rdd_52 = spark.sparkContext.union([task_rdd_44])
    task_rdd_52 = simulate_task(union_rdd_52, '52', 59)

    union_rdd_19 = spark.sparkContext.union([task_rdd_52])
    task_rdd_19 = simulate_task(union_rdd_19, '19', 127)

    union_rdd_43 = spark.sparkContext.union([task_rdd_42])
    task_rdd_43 = simulate_task(union_rdd_43, '43', 20)

    union_rdd_131 = spark.sparkContext.union([task_rdd_130])
    task_rdd_131 = simulate_task(union_rdd_131, '131', 3)

    union_rdd_100 = spark.sparkContext.union([task_rdd_93])
    task_rdd_100 = simulate_task(union_rdd_100, '100', 13)

    union_rdd_60 = spark.sparkContext.union([task_rdd_54, task_rdd_56, task_rdd_58, task_rdd_59])
    task_rdd_60 = simulate_task(union_rdd_60, '60', 58)

    union_rdd_23 = spark.sparkContext.union([task_rdd_15, task_rdd_19, task_rdd_22])
    task_rdd_23 = simulate_task(union_rdd_23, '23', 70)

    union_rdd_18 = spark.sparkContext.union([task_rdd_19])
    task_rdd_18 = simulate_task(union_rdd_18, '18', 1)

    union_rdd_20 = spark.sparkContext.union([task_rdd_43])
    task_rdd_20 = simulate_task(union_rdd_20, '20', 25)

    union_rdd_132 = spark.sparkContext.union([task_rdd_131])
    task_rdd_132 = simulate_task(union_rdd_132, '132', 1)

    union_rdd_67 = spark.sparkContext.union([task_rdd_100])
    task_rdd_67 = simulate_task(union_rdd_67, '67', 14)

    union_rdd_55 = spark.sparkContext.union([task_rdd_60])
    task_rdd_55 = simulate_task(union_rdd_55, '55', 29)

    union_rdd_7 = spark.sparkContext.union([task_rdd_4, task_rdd_5, task_rdd_6, task_rdd_13, task_rdd_18])
    task_rdd_7 = simulate_task(union_rdd_7, '7', 54)

    union_rdd_29 = spark.sparkContext.union([task_rdd_8, task_rdd_12, task_rdd_16, task_rdd_20, task_rdd_28])
    task_rdd_29 = simulate_task(union_rdd_29, '29', 10)

    union_rdd_120 = spark.sparkContext.union([task_rdd_132])
    task_rdd_120 = simulate_task(union_rdd_120, '120', 4)

    union_rdd_68 = spark.sparkContext.union([task_rdd_55, task_rdd_67])
    task_rdd_68 = simulate_task(union_rdd_68, '68', 29)

    union_rdd_64 = spark.sparkContext.union([task_rdd_120])
    task_rdd_64 = simulate_task(union_rdd_64, '64', 4)

    union_rdd_63 = spark.sparkContext.union([task_rdd_68])
    task_rdd_63 = simulate_task(union_rdd_63, '63', 64)

    union_rdd_65 = spark.sparkContext.union([task_rdd_64])
    task_rdd_65 = simulate_task(union_rdd_65, '65', 21)

    union_rdd_66 = spark.sparkContext.union([task_rdd_34, task_rdd_38, task_rdd_40, task_rdd_63, task_rdd_65])
    task_rdd_66 = simulate_task(union_rdd_66, '66', 7)

    union_rdd_26 = spark.sparkContext.union([task_rdd_66])
    task_rdd_26 = simulate_task(union_rdd_26, '26', 89)

    union_rdd_27 = spark.sparkContext.union([task_rdd_26])
    task_rdd_27 = simulate_task(union_rdd_27, '27', 20)

    union_rdd_30 = spark.sparkContext.union([task_rdd_26, task_rdd_27, task_rdd_28, task_rdd_29])
    task_rdd_30 = simulate_task(union_rdd_30, '30', 2)

    union_rdd_46 = spark.sparkContext.union([task_rdd_27])
    task_rdd_46 = simulate_task(union_rdd_46, '46', 40)

    union_rdd_9 = spark.sparkContext.union([task_rdd_12, task_rdd_30])
    task_rdd_9 = simulate_task(union_rdd_9, '9', 13)

    union_rdd_35 = spark.sparkContext.union([task_rdd_46])
    task_rdd_35 = simulate_task(union_rdd_35, '35', 69)

    union_rdd_36 = spark.sparkContext.union([task_rdd_35])
    task_rdd_36 = simulate_task(union_rdd_36, '36', 28)

    combined_rdd = spark.sparkContext.union([task_rdd_3, task_rdd_4, task_rdd_12, task_rdd_14, task_rdd_19, task_rdd_21, task_rdd_22, task_rdd_23])
    task_rdd = simulate_task(combined_rdd, 'J15_3_4_12_14_19_21_22_23', 136)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
