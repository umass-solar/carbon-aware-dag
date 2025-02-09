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

    task_rdd_19 = simulate_task(base_rdd, '19', 116)
    task_rdd_16 = simulate_task(base_rdd, '16', 1)
    task_rdd_15 = simulate_task(base_rdd, '15', 5)
    task_rdd_14 = simulate_task(base_rdd, '14', 7)
    task_rdd_13 = simulate_task(base_rdd, '13', 5)
    task_rdd_1 = simulate_task(base_rdd, '1', 7)
    task_rdd_8 = simulate_task(base_rdd, '8', 7)
    task_rdd_5 = simulate_task(base_rdd, '5', 6)
    task_rdd_10 = simulate_task(base_rdd, '10', 4)
    task_rdd_3 = simulate_task(base_rdd, '3', 40)
    task_rdd_4 = simulate_task(base_rdd, '4', 17)
    task_rdd_11 = simulate_task(base_rdd, '11', 15)
    task_rdd_12 = simulate_task(base_rdd, '12', 3)
    task_rdd_6 = simulate_task(base_rdd, '6', 1)
    task_rdd_7 = simulate_task(base_rdd, '7', 2)
    task_rdd_33 = simulate_task(base_rdd, '33', 8)
    task_rdd_49 = simulate_task(base_rdd, '49', 108)
    task_rdd_24 = simulate_task(base_rdd, '24', 12)
    task_rdd_28 = simulate_task(base_rdd, '28', 2)
    task_rdd_31 = simulate_task(base_rdd, '31', 161)
    task_rdd_30 = simulate_task(base_rdd, '30', 3)
    task_rdd_40 = simulate_task(base_rdd, '40', 237)
    task_rdd_53 = simulate_task(base_rdd, '53', 3)
    task_rdd_51 = simulate_task(base_rdd, '51', 5)
    task_rdd_52 = simulate_task(base_rdd, '52', 58)
    task_rdd_50 = simulate_task(base_rdd, '50', 29)
    task_rdd_120 = simulate_task(base_rdd, '120', 36)
    task_rdd_57 = simulate_task(base_rdd, '57', 66)
    task_rdd_55 = simulate_task(base_rdd, '55', 66)
    task_rdd_78 = simulate_task(base_rdd, '78', 3)
    task_rdd_80 = simulate_task(base_rdd, '80', 8)
    task_rdd_61 = simulate_task(base_rdd, '61', 68)
    task_rdd_75 = simulate_task(base_rdd, '75', 5)
    task_rdd_73 = simulate_task(base_rdd, '73', 6)
    task_rdd_72 = simulate_task(base_rdd, '72', 4)
    task_rdd_131 = simulate_task(base_rdd, '131', 4)

    union_rdd_17 = spark.sparkContext.union([task_rdd_4, task_rdd_8, task_rdd_12, task_rdd_14, task_rdd_16])
    task_rdd_17 = simulate_task(union_rdd_17, '17', 220)

    union_rdd_20 = spark.sparkContext.union([task_rdd_11])
    task_rdd_20 = simulate_task(union_rdd_20, '20', 73)

    union_rdd_29 = spark.sparkContext.union([task_rdd_24])
    task_rdd_29 = simulate_task(union_rdd_29, '29', 29)

    union_rdd_41 = spark.sparkContext.union([task_rdd_40])
    task_rdd_41 = simulate_task(union_rdd_41, '41', 634)

    union_rdd_54 = spark.sparkContext.union([task_rdd_53])
    task_rdd_54 = simulate_task(union_rdd_54, '54', 18)

    union_rdd_62 = spark.sparkContext.union([task_rdd_50])
    task_rdd_62 = simulate_task(union_rdd_62, '62', 22)

    union_rdd_58 = spark.sparkContext.union([task_rdd_120])
    task_rdd_58 = simulate_task(union_rdd_58, '58', 4)

    union_rdd_83 = spark.sparkContext.union([task_rdd_80])
    task_rdd_83 = simulate_task(union_rdd_83, '83', 491)

    union_rdd_81 = spark.sparkContext.union([task_rdd_61])
    task_rdd_81 = simulate_task(union_rdd_81, '81', 35)

    union_rdd_79 = spark.sparkContext.union([task_rdd_75])
    task_rdd_79 = simulate_task(union_rdd_79, '79', 6)

    union_rdd_74 = spark.sparkContext.union([task_rdd_73])
    task_rdd_74 = simulate_task(union_rdd_74, '74', 10)

    union_rdd_132 = spark.sparkContext.union([task_rdd_131])
    task_rdd_132 = simulate_task(union_rdd_132, '132', 1)

    union_rdd_18 = spark.sparkContext.union([task_rdd_3, task_rdd_16, task_rdd_17])
    task_rdd_18 = simulate_task(union_rdd_18, '18', 94)

    union_rdd_21 = spark.sparkContext.union([task_rdd_19, task_rdd_20])
    task_rdd_21 = simulate_task(union_rdd_21, '21', 2)

    union_rdd_32 = spark.sparkContext.union([task_rdd_29, task_rdd_49])
    task_rdd_32 = simulate_task(union_rdd_32, '32', 17)

    union_rdd_56 = spark.sparkContext.union([task_rdd_54])
    task_rdd_56 = simulate_task(union_rdd_56, '56', 7)

    union_rdd_42 = spark.sparkContext.union([task_rdd_41])
    task_rdd_42 = simulate_task(union_rdd_42, '42', 152)

    union_rdd_59 = spark.sparkContext.union([task_rdd_55, task_rdd_57, task_rdd_58])
    task_rdd_59 = simulate_task(union_rdd_59, '59', 190)

    union_rdd_82 = spark.sparkContext.union([task_rdd_78, task_rdd_81])
    task_rdd_82 = simulate_task(union_rdd_82, '82', 102)

    union_rdd_22 = spark.sparkContext.union([task_rdd_6, task_rdd_12, task_rdd_21])
    task_rdd_22 = simulate_task(union_rdd_22, '22', 15)

    union_rdd_34 = spark.sparkContext.union([task_rdd_56])
    task_rdd_34 = simulate_task(union_rdd_34, '34', 3)

    union_rdd_60 = spark.sparkContext.union([task_rdd_59])
    task_rdd_60 = simulate_task(union_rdd_60, '60', 6)

    union_rdd_84 = spark.sparkContext.union([task_rdd_82, task_rdd_83])
    task_rdd_84 = simulate_task(union_rdd_84, '84', 102)

    union_rdd_23 = spark.sparkContext.union([task_rdd_22])
    task_rdd_23 = simulate_task(union_rdd_23, '23', 12)

    union_rdd_35 = spark.sparkContext.union([task_rdd_34])
    task_rdd_35 = simulate_task(union_rdd_35, '35', 9)

    union_rdd_85 = spark.sparkContext.union([task_rdd_75, task_rdd_79, task_rdd_81, task_rdd_83, task_rdd_84])
    task_rdd_85 = simulate_task(union_rdd_85, '85', 72)

    union_rdd_39 = spark.sparkContext.union([task_rdd_35])
    task_rdd_39 = simulate_task(union_rdd_39, '39', 72)

    union_rdd_86 = spark.sparkContext.union([task_rdd_85])
    task_rdd_86 = simulate_task(union_rdd_86, '86', 77)

    union_rdd_48 = spark.sparkContext.union([task_rdd_39])
    task_rdd_48 = simulate_task(union_rdd_48, '48', 2)

    union_rdd_87 = spark.sparkContext.union([task_rdd_85, task_rdd_86])
    task_rdd_87 = simulate_task(union_rdd_87, '87', 1)

    union_rdd_45 = spark.sparkContext.union([task_rdd_48])
    task_rdd_45 = simulate_task(union_rdd_45, '45', 4)

    union_rdd_88 = spark.sparkContext.union([task_rdd_87])
    task_rdd_88 = simulate_task(union_rdd_88, '88', 21)

    union_rdd_46 = spark.sparkContext.union([task_rdd_30, task_rdd_31, task_rdd_34, task_rdd_35, task_rdd_45])
    task_rdd_46 = simulate_task(union_rdd_46, '46', 77)

    union_rdd_76 = spark.sparkContext.union([task_rdd_88])
    task_rdd_76 = simulate_task(union_rdd_76, '76', 3)

    union_rdd_47 = spark.sparkContext.union([task_rdd_46])
    task_rdd_47 = simulate_task(union_rdd_47, '47', 241)

    union_rdd_77 = spark.sparkContext.union([task_rdd_72, task_rdd_73, task_rdd_74, task_rdd_76])
    task_rdd_77 = simulate_task(union_rdd_77, '77', 3)

    union_rdd_2 = spark.sparkContext.union([task_rdd_77])
    task_rdd_2 = simulate_task(union_rdd_2, '2', 49)

    union_rdd_64 = spark.sparkContext.union([task_rdd_47])
    task_rdd_64 = simulate_task(union_rdd_64, '64', 287)

    union_rdd_43 = spark.sparkContext.union([task_rdd_1, task_rdd_2, task_rdd_3, task_rdd_4, task_rdd_5, task_rdd_6, task_rdd_7, task_rdd_8, task_rdd_10, task_rdd_11, task_rdd_12, task_rdd_1])
    task_rdd_43 = simulate_task(union_rdd_43, '43', 14)

    union_rdd_65 = spark.sparkContext.union([task_rdd_52, task_rdd_56, task_rdd_64])
    task_rdd_65 = simulate_task(union_rdd_65, '65', 264)

    union_rdd_44 = spark.sparkContext.union([task_rdd_3, task_rdd_43])
    task_rdd_44 = simulate_task(union_rdd_44, '44', 10)

    union_rdd_66 = spark.sparkContext.union([task_rdd_53, task_rdd_62, task_rdd_65])
    task_rdd_66 = simulate_task(union_rdd_66, '66', 124)

    union_rdd_67 = spark.sparkContext.union([task_rdd_44])
    task_rdd_67 = simulate_task(union_rdd_67, '67', 23)

    union_rdd_63 = spark.sparkContext.union([task_rdd_60, task_rdd_62, task_rdd_66])
    task_rdd_63 = simulate_task(union_rdd_63, '63', 21)

    union_rdd_25 = spark.sparkContext.union([task_rdd_63])
    task_rdd_25 = simulate_task(union_rdd_25, '25', 6)

    union_rdd_26 = spark.sparkContext.union([task_rdd_25])
    task_rdd_26 = simulate_task(union_rdd_26, '26', 6)

    union_rdd_27 = spark.sparkContext.union([task_rdd_26])
    task_rdd_27 = simulate_task(union_rdd_27, '27', 109)

    union_rdd_9 = spark.sparkContext.union([task_rdd_26, task_rdd_27, task_rdd_32])
    task_rdd_9 = simulate_task(union_rdd_9, '9', 15)

    combined_rdd = spark.sparkContext.union([task_rdd_7, task_rdd_8, task_rdd_14, task_rdd_15, task_rdd_18])
    task_rdd = simulate_task(combined_rdd, 'J19_7_8_14_15_18', 2)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
