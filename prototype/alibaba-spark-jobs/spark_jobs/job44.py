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

    task_rdd_15 = simulate_task(base_rdd, '15', 26)
    task_rdd_132 = simulate_task(base_rdd, '132', 4)
    task_rdd_5 = simulate_task(base_rdd, '5', 4)
    task_rdd_7 = simulate_task(base_rdd, '7', 28)
    task_rdd_11 = simulate_task(base_rdd, '11', 53)
    task_rdd_20 = simulate_task(base_rdd, '20', 4)
    task_rdd_13 = simulate_task(base_rdd, '13', 2)
    task_rdd_3 = simulate_task(base_rdd, '3', 87)
    task_rdd_8 = simulate_task(base_rdd, '8', 16)
    task_rdd_77 = simulate_task(base_rdd, '77', 5)
    task_rdd_24 = simulate_task(base_rdd, '24', 61)
    task_rdd_23 = simulate_task(base_rdd, '23', 22)
    task_rdd_12 = simulate_task(base_rdd, '12', 44)
    task_rdd_1 = simulate_task(base_rdd, '1', 24)
    task_rdd_89 = simulate_task(base_rdd, '89', 2)
    task_rdd_42 = simulate_task(base_rdd, '42', 95)
    task_rdd_54 = simulate_task(base_rdd, '54', 35)
    task_rdd_63 = simulate_task(base_rdd, '63', 4)
    task_rdd_57 = simulate_task(base_rdd, '57', 2)
    task_rdd_44 = simulate_task(base_rdd, '44', 3)
    task_rdd_45 = simulate_task(base_rdd, '45', 61)
    task_rdd_56 = simulate_task(base_rdd, '56', 32)
    task_rdd_80 = simulate_task(base_rdd, '80', 8)
    task_rdd_73 = simulate_task(base_rdd, '73', 2)
    task_rdd_62 = simulate_task(base_rdd, '62', 13)
    task_rdd_43 = simulate_task(base_rdd, '43', 71)
    task_rdd_67 = simulate_task(base_rdd, '67', 81)
    task_rdd_31 = simulate_task(base_rdd, '31', 46)
    task_rdd_26 = simulate_task(base_rdd, '26', 9)

    union_rdd_16 = spark.sparkContext.union([task_rdd_11, task_rdd_15])
    task_rdd_16 = simulate_task(union_rdd_16, '16', 140)

    union_rdd_120 = spark.sparkContext.union([task_rdd_132])
    task_rdd_120 = simulate_task(union_rdd_120, '120', 4)

    union_rdd_22 = spark.sparkContext.union([task_rdd_20])
    task_rdd_22 = simulate_task(union_rdd_22, '22', 50)

    union_rdd_9 = spark.sparkContext.union([task_rdd_8])
    task_rdd_9 = simulate_task(union_rdd_9, '9', 90)

    union_rdd_58 = spark.sparkContext.union([task_rdd_77])
    task_rdd_58 = simulate_task(union_rdd_58, '58', 3)

    union_rdd_25 = spark.sparkContext.union([task_rdd_12, task_rdd_23, task_rdd_24])
    task_rdd_25 = simulate_task(union_rdd_25, '25', 160)

    union_rdd_55 = spark.sparkContext.union([task_rdd_54])
    task_rdd_55 = simulate_task(union_rdd_55, '55', 3)

    union_rdd_59 = spark.sparkContext.union([task_rdd_63])
    task_rdd_59 = simulate_task(union_rdd_59, '59', 3)

    union_rdd_34 = spark.sparkContext.union([task_rdd_44, task_rdd_57])
    task_rdd_34 = simulate_task(union_rdd_34, '34', 22)

    union_rdd_30 = spark.sparkContext.union([task_rdd_26])
    task_rdd_30 = simulate_task(union_rdd_30, '30', 2)

    union_rdd_86 = spark.sparkContext.union([task_rdd_56])
    task_rdd_86 = simulate_task(union_rdd_86, '86', 2)

    union_rdd_81 = spark.sparkContext.union([task_rdd_62, task_rdd_73, task_rdd_80])
    task_rdd_81 = simulate_task(union_rdd_81, '81', 252)

    union_rdd_79 = spark.sparkContext.union([task_rdd_43])
    task_rdd_79 = simulate_task(union_rdd_79, '79', 95)

    union_rdd_70 = spark.sparkContext.union([task_rdd_67])
    task_rdd_70 = simulate_task(union_rdd_70, '70', 72)

    union_rdd_17 = spark.sparkContext.union([task_rdd_7, task_rdd_15, task_rdd_16])
    task_rdd_17 = simulate_task(union_rdd_17, '17', 44)

    union_rdd_6 = spark.sparkContext.union([task_rdd_120])
    task_rdd_6 = simulate_task(union_rdd_6, '6', 4)

    union_rdd_40 = spark.sparkContext.union([task_rdd_22])
    task_rdd_40 = simulate_task(union_rdd_40, '40', 4)

    union_rdd_53 = spark.sparkContext.union([task_rdd_1, task_rdd_12, task_rdd_24, task_rdd_25])
    task_rdd_53 = simulate_task(union_rdd_53, '53', 106)

    union_rdd_46 = spark.sparkContext.union([task_rdd_59])
    task_rdd_46 = simulate_task(union_rdd_46, '46', 6)

    union_rdd_35 = spark.sparkContext.union([task_rdd_34])
    task_rdd_35 = simulate_task(union_rdd_35, '35', 84)

    union_rdd_85 = spark.sparkContext.union([task_rdd_86])
    task_rdd_85 = simulate_task(union_rdd_85, '85', 11)

    union_rdd_71 = spark.sparkContext.union([task_rdd_70])
    task_rdd_71 = simulate_task(union_rdd_71, '71', 72)

    union_rdd_41 = spark.sparkContext.union([task_rdd_3, task_rdd_40])
    task_rdd_41 = simulate_task(union_rdd_41, '41', 688)

    union_rdd_36 = spark.sparkContext.union([task_rdd_35])
    task_rdd_36 = simulate_task(union_rdd_36, '36', 92)

    union_rdd_76 = spark.sparkContext.union([task_rdd_79, task_rdd_81, task_rdd_85])
    task_rdd_76 = simulate_task(union_rdd_76, '76', 7)

    union_rdd_87 = spark.sparkContext.union([task_rdd_41])
    task_rdd_87 = simulate_task(union_rdd_87, '87', 10)

    union_rdd_47 = spark.sparkContext.union([task_rdd_30, task_rdd_36, task_rdd_46])
    task_rdd_47 = simulate_task(union_rdd_47, '47', 472)

    union_rdd_78 = spark.sparkContext.union([task_rdd_71, task_rdd_76])
    task_rdd_78 = simulate_task(union_rdd_78, '78', 83)

    union_rdd_60 = spark.sparkContext.union([task_rdd_87])
    task_rdd_60 = simulate_task(union_rdd_60, '60', 5)

    union_rdd_48 = spark.sparkContext.union([task_rdd_47, task_rdd_55])
    task_rdd_48 = simulate_task(union_rdd_48, '48', 15)

    union_rdd_61 = spark.sparkContext.union([task_rdd_53, task_rdd_58, task_rdd_60])
    task_rdd_61 = simulate_task(union_rdd_61, '61', 4)

    union_rdd_49 = spark.sparkContext.union([task_rdd_48])
    task_rdd_49 = simulate_task(union_rdd_49, '49', 148)

    union_rdd_2 = spark.sparkContext.union([task_rdd_61])
    task_rdd_2 = simulate_task(union_rdd_2, '2', 258)

    union_rdd_50 = spark.sparkContext.union([task_rdd_49])
    task_rdd_50 = simulate_task(union_rdd_50, '50', 10)

    union_rdd_10 = spark.sparkContext.union([task_rdd_2])
    task_rdd_10 = simulate_task(union_rdd_10, '10', 5)

    union_rdd_52 = spark.sparkContext.union([task_rdd_50])
    task_rdd_52 = simulate_task(union_rdd_52, '52', 67)

    union_rdd_51 = spark.sparkContext.union([task_rdd_50, task_rdd_52])
    task_rdd_51 = simulate_task(union_rdd_51, '51', 48)

    union_rdd_90 = spark.sparkContext.union([task_rdd_51, task_rdd_89])
    task_rdd_90 = simulate_task(union_rdd_90, '90', 17)

    union_rdd_91 = spark.sparkContext.union([task_rdd_26, task_rdd_30, task_rdd_31, task_rdd_78, task_rdd_90])
    task_rdd_91 = simulate_task(union_rdd_91, '91', 813)

    union_rdd_95 = spark.sparkContext.union([task_rdd_91])
    task_rdd_95 = simulate_task(union_rdd_95, '95', 72)

    union_rdd_100 = spark.sparkContext.union([task_rdd_95])
    task_rdd_100 = simulate_task(union_rdd_100, '100', 2)

    union_rdd_27 = spark.sparkContext.union([task_rdd_100])
    task_rdd_27 = simulate_task(union_rdd_27, '27', 15)

    union_rdd_28 = spark.sparkContext.union([task_rdd_27])
    task_rdd_28 = simulate_task(union_rdd_28, '28', 8)

    union_rdd_4 = spark.sparkContext.union([task_rdd_28])
    task_rdd_4 = simulate_task(union_rdd_4, '4', 1)

    union_rdd_18 = spark.sparkContext.union([task_rdd_4, task_rdd_8, task_rdd_11, task_rdd_13, task_rdd_15, task_rdd_17])
    task_rdd_18 = simulate_task(union_rdd_18, '18', 207)

    union_rdd_19 = spark.sparkContext.union([task_rdd_5, task_rdd_12, task_rdd_18])
    task_rdd_19 = simulate_task(union_rdd_19, '19', 29)

    union_rdd_38 = spark.sparkContext.union([task_rdd_19])
    task_rdd_38 = simulate_task(union_rdd_38, '38', 5)

    union_rdd_39 = spark.sparkContext.union([task_rdd_1, task_rdd_38])
    task_rdd_39 = simulate_task(union_rdd_39, '39', 715)

    union_rdd_21 = spark.sparkContext.union([task_rdd_20, task_rdd_39])
    task_rdd_21 = simulate_task(union_rdd_21, '21', 20)

    union_rdd_14 = spark.sparkContext.union([task_rdd_12, task_rdd_21])
    task_rdd_14 = simulate_task(union_rdd_14, '14', 205)

    combined_rdd = spark.sparkContext.union([task_rdd_13, task_rdd_14])
    task_rdd = simulate_task(combined_rdd, 'J15_13_14', 12)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
