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

    task_rdd_12 = simulate_task(base_rdd, '12', 10)
    task_rdd_10 = simulate_task(base_rdd, '10', 23)
    task_rdd_9 = simulate_task(base_rdd, '9', 16)
    task_rdd_8 = simulate_task(base_rdd, '8', 1)
    task_rdd_7 = simulate_task(base_rdd, '7', 5)
    task_rdd_4 = simulate_task(base_rdd, '4', 1)
    task_rdd_2 = simulate_task(base_rdd, '2', 351)
    task_rdd_6 = simulate_task(base_rdd, '6', 2)
    task_rdd_3 = simulate_task(base_rdd, '3', 1)
    task_rdd_5 = simulate_task(base_rdd, '5', 3)
    task_rdd_16 = simulate_task(base_rdd, '16', 71)
    task_rdd_1 = simulate_task(base_rdd, '1', 1)
    task_rdd_29 = simulate_task(base_rdd, '29', 2)
    task_rdd_26 = simulate_task(base_rdd, '26', 270)
    task_rdd_24 = simulate_task(base_rdd, '24', 1)
    task_rdd_23 = simulate_task(base_rdd, '23', 7)
    task_rdd_20 = simulate_task(base_rdd, '20', 8)
    task_rdd_78 = simulate_task(base_rdd, '78', 2)
    task_rdd_34 = simulate_task(base_rdd, '34', 11)
    task_rdd_33 = simulate_task(base_rdd, '33', 34)
    task_rdd_31 = simulate_task(base_rdd, '31', 51)
    task_rdd_19 = simulate_task(base_rdd, '19', 23)
    task_rdd_18 = simulate_task(base_rdd, '18', 3)
    task_rdd_38 = simulate_task(base_rdd, '38', 5)
    task_rdd_63 = simulate_task(base_rdd, '63', 18)
    task_rdd_120 = simulate_task(base_rdd, '120', 36)
    task_rdd_129 = simulate_task(base_rdd, '129', 4)
    task_rdd_99 = simulate_task(base_rdd, '99', 4)
    task_rdd_54 = simulate_task(base_rdd, '54', 10)
    task_rdd_43 = simulate_task(base_rdd, '43', 4)
    task_rdd_39 = simulate_task(base_rdd, '39', 14)
    task_rdd_42 = simulate_task(base_rdd, '42', 30)
    task_rdd_41 = simulate_task(base_rdd, '41', 32)

    union_rdd_13 = spark.sparkContext.union([task_rdd_4, task_rdd_6, task_rdd_7, task_rdd_9, task_rdd_12])
    task_rdd_13 = simulate_task(union_rdd_13, '13', 12)

    union_rdd_11 = spark.sparkContext.union([task_rdd_2, task_rdd_6, task_rdd_7, task_rdd_8, task_rdd_10])
    task_rdd_11 = simulate_task(union_rdd_11, '11', 124)

    union_rdd_17 = spark.sparkContext.union([task_rdd_16])
    task_rdd_17 = simulate_task(union_rdd_17, '17', 133)

    union_rdd_27 = spark.sparkContext.union([task_rdd_20, task_rdd_23, task_rdd_26])
    task_rdd_27 = simulate_task(union_rdd_27, '27', 270)

    union_rdd_21 = spark.sparkContext.union([task_rdd_38])
    task_rdd_21 = simulate_task(union_rdd_21, '21', 12)

    union_rdd_32 = spark.sparkContext.union([task_rdd_19, task_rdd_31])
    task_rdd_32 = simulate_task(union_rdd_32, '32', 118)

    union_rdd_76 = spark.sparkContext.union([task_rdd_120])
    task_rdd_76 = simulate_task(union_rdd_76, '76', 4)

    union_rdd_130 = spark.sparkContext.union([task_rdd_129])
    task_rdd_130 = simulate_task(union_rdd_130, '130', 1)

    union_rdd_67 = spark.sparkContext.union([task_rdd_99])
    task_rdd_67 = simulate_task(union_rdd_67, '67', 8)

    union_rdd_56 = spark.sparkContext.union([task_rdd_42, task_rdd_43])
    task_rdd_56 = simulate_task(union_rdd_56, '56', 81)

    union_rdd_55 = spark.sparkContext.union([task_rdd_54])
    task_rdd_55 = simulate_task(union_rdd_55, '55', 86)

    union_rdd_40 = spark.sparkContext.union([task_rdd_39])
    task_rdd_40 = simulate_task(union_rdd_40, '40', 40)

    union_rdd_14 = spark.sparkContext.union([task_rdd_3, task_rdd_6, task_rdd_9, task_rdd_12, task_rdd_13])
    task_rdd_14 = simulate_task(union_rdd_14, '14', 24)

    union_rdd_28 = spark.sparkContext.union([task_rdd_23, task_rdd_26, task_rdd_27])
    task_rdd_28 = simulate_task(union_rdd_28, '28', 24)

    union_rdd_25 = spark.sparkContext.union([task_rdd_18, task_rdd_20, task_rdd_21, task_rdd_23, task_rdd_24])
    task_rdd_25 = simulate_task(union_rdd_25, '25', 9)

    union_rdd_22 = spark.sparkContext.union([task_rdd_20, task_rdd_21])
    task_rdd_22 = simulate_task(union_rdd_22, '22', 15)

    union_rdd_35 = spark.sparkContext.union([task_rdd_32, task_rdd_33, task_rdd_34])
    task_rdd_35 = simulate_task(union_rdd_35, '35', 134)

    union_rdd_131 = spark.sparkContext.union([task_rdd_130])
    task_rdd_131 = simulate_task(union_rdd_131, '131', 3)

    union_rdd_58 = spark.sparkContext.union([task_rdd_67])
    task_rdd_58 = simulate_task(union_rdd_58, '58', 34)

    union_rdd_61 = spark.sparkContext.union([task_rdd_56])
    task_rdd_61 = simulate_task(union_rdd_61, '61', 12)

    union_rdd_15 = spark.sparkContext.union([task_rdd_3, task_rdd_14])
    task_rdd_15 = simulate_task(union_rdd_15, '15', 10)

    union_rdd_30 = spark.sparkContext.union([task_rdd_18, task_rdd_28, task_rdd_29])
    task_rdd_30 = simulate_task(union_rdd_30, '30', 7)

    union_rdd_36 = spark.sparkContext.union([task_rdd_34, task_rdd_35])
    task_rdd_36 = simulate_task(union_rdd_36, '36', 74)

    union_rdd_132 = spark.sparkContext.union([task_rdd_131])
    task_rdd_132 = simulate_task(union_rdd_132, '132', 1)

    union_rdd_57 = spark.sparkContext.union([task_rdd_61])
    task_rdd_57 = simulate_task(union_rdd_57, '57', 2)

    union_rdd_37 = spark.sparkContext.union([task_rdd_36])
    task_rdd_37 = simulate_task(union_rdd_37, '37', 440)

    union_rdd_74 = spark.sparkContext.union([task_rdd_132])
    task_rdd_74 = simulate_task(union_rdd_74, '74', 4)

    union_rdd_59 = spark.sparkContext.union([task_rdd_56, task_rdd_57, task_rdd_58])
    task_rdd_59 = simulate_task(union_rdd_59, '59', 1232)

    union_rdd_47 = spark.sparkContext.union([task_rdd_37])
    task_rdd_47 = simulate_task(union_rdd_47, '47', 38)

    union_rdd_75 = spark.sparkContext.union([task_rdd_74])
    task_rdd_75 = simulate_task(union_rdd_75, '75', 502)

    union_rdd_60 = spark.sparkContext.union([task_rdd_59])
    task_rdd_60 = simulate_task(union_rdd_60, '60', 6)

    union_rdd_48 = spark.sparkContext.union([task_rdd_47])
    task_rdd_48 = simulate_task(union_rdd_48, '48', 132)

    union_rdd_77 = spark.sparkContext.union([task_rdd_75, task_rdd_76])
    task_rdd_77 = simulate_task(union_rdd_77, '77', 120)

    union_rdd_68 = spark.sparkContext.union([task_rdd_60])
    task_rdd_68 = simulate_task(union_rdd_68, '68', 243)

    union_rdd_79 = spark.sparkContext.union([task_rdd_48, task_rdd_78])
    task_rdd_79 = simulate_task(union_rdd_79, '79', 25)

    union_rdd_69 = spark.sparkContext.union([task_rdd_68])
    task_rdd_69 = simulate_task(union_rdd_69, '69', 489)

    union_rdd_71 = spark.sparkContext.union([task_rdd_79])
    task_rdd_71 = simulate_task(union_rdd_71, '71', 295)

    union_rdd_44 = spark.sparkContext.union([task_rdd_42, task_rdd_43, task_rdd_69, task_rdd_77])
    task_rdd_44 = simulate_task(union_rdd_44, '44', 19)

    union_rdd_73 = spark.sparkContext.union([])
    task_rdd_73 = simulate_task(union_rdd_73, '73', 8)

    combined_rdd = spark.sparkContext.union([task_rdd_15])
    task_rdd = simulate_task(combined_rdd, 'M45_15', 14)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
