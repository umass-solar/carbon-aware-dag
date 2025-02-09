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

    task_rdd_13 = simulate_task(base_rdd, '13', 1)
    task_rdd_9 = simulate_task(base_rdd, '9', 81)
    task_rdd_31 = simulate_task(base_rdd, '31', 14)
    task_rdd_28 = simulate_task(base_rdd, '28', 64)
    task_rdd_6 = simulate_task(base_rdd, '6', 19)
    task_rdd_15 = simulate_task(base_rdd, '15', 342)
    task_rdd_5 = simulate_task(base_rdd, '5', 1)
    task_rdd_38 = simulate_task(base_rdd, '38', 42)
    task_rdd_35 = simulate_task(base_rdd, '35', 103)
    task_rdd_3 = simulate_task(base_rdd, '3', 1)
    task_rdd_22 = simulate_task(base_rdd, '22', 126)
    task_rdd_131 = simulate_task(base_rdd, '131', 1)
    task_rdd_1 = simulate_task(base_rdd, '1', 25)
    task_rdd_18 = simulate_task(base_rdd, '18', 10)
    task_rdd_2 = simulate_task(base_rdd, '2', 3)
    task_rdd_16 = simulate_task(base_rdd, '16', 8)
    task_rdd_14 = simulate_task(base_rdd, '14', 6)
    task_rdd_24 = simulate_task(base_rdd, '24', 82)
    task_rdd_20 = simulate_task(base_rdd, '20', 15)
    task_rdd_19 = simulate_task(base_rdd, '19', 11)
    task_rdd_8 = simulate_task(base_rdd, '8', 18)
    task_rdd_43 = simulate_task(base_rdd, '43', 25)
    task_rdd_99 = simulate_task(base_rdd, '99', 3)
    task_rdd_80 = simulate_task(base_rdd, '80', 3)
    task_rdd_76 = simulate_task(base_rdd, '76', 3)
    task_rdd_44 = simulate_task(base_rdd, '44', 1)
    task_rdd_42 = simulate_task(base_rdd, '42', 5)

    union_rdd_10 = spark.sparkContext.union([task_rdd_2, task_rdd_3, task_rdd_5, task_rdd_6, task_rdd_8, task_rdd_9])
    task_rdd_10 = simulate_task(union_rdd_10, '10', 47)

    union_rdd_29 = spark.sparkContext.union([task_rdd_5, task_rdd_15, task_rdd_19, task_rdd_20, task_rdd_28])
    task_rdd_29 = simulate_task(union_rdd_29, '29', 38)

    union_rdd_26 = spark.sparkContext.union([task_rdd_5, task_rdd_6])
    task_rdd_26 = simulate_task(union_rdd_26, '26', 26)

    union_rdd_39 = spark.sparkContext.union([task_rdd_3, task_rdd_35, task_rdd_38])
    task_rdd_39 = simulate_task(union_rdd_39, '39', 448)

    union_rdd_23 = spark.sparkContext.union([task_rdd_22])
    task_rdd_23 = simulate_task(union_rdd_23, '23', 211)

    union_rdd_132 = spark.sparkContext.union([task_rdd_131])
    task_rdd_132 = simulate_task(union_rdd_132, '132', 1)

    union_rdd_59 = spark.sparkContext.union([task_rdd_1])
    task_rdd_59 = simulate_task(union_rdd_59, '59', 162)

    union_rdd_77 = spark.sparkContext.union([task_rdd_76])
    task_rdd_77 = simulate_task(union_rdd_77, '77', 85)

    union_rdd_45 = spark.sparkContext.union([task_rdd_42, task_rdd_44])
    task_rdd_45 = simulate_task(union_rdd_45, '45', 72)

    union_rdd_30 = spark.sparkContext.union([task_rdd_28, task_rdd_29, task_rdd_31])
    task_rdd_30 = simulate_task(union_rdd_30, '30', 19)

    union_rdd_72 = spark.sparkContext.union([task_rdd_132])
    task_rdd_72 = simulate_task(union_rdd_72, '72', 4)

    union_rdd_60 = spark.sparkContext.union([task_rdd_59])
    task_rdd_60 = simulate_task(union_rdd_60, '60', 51)

    union_rdd_97 = spark.sparkContext.union([task_rdd_77, task_rdd_80])
    task_rdd_97 = simulate_task(union_rdd_97, '97', 4)

    union_rdd_17 = spark.sparkContext.union([task_rdd_16, task_rdd_30])
    task_rdd_17 = simulate_task(union_rdd_17, '17', 30)

    union_rdd_73 = spark.sparkContext.union([task_rdd_72])
    task_rdd_73 = simulate_task(union_rdd_73, '73', 16)

    union_rdd_54 = spark.sparkContext.union([task_rdd_60])
    task_rdd_54 = simulate_task(union_rdd_54, '54', 101)

    union_rdd_98 = spark.sparkContext.union([task_rdd_45, task_rdd_97])
    task_rdd_98 = simulate_task(union_rdd_98, '98', 23)

    union_rdd_36 = spark.sparkContext.union([task_rdd_17])
    task_rdd_36 = simulate_task(union_rdd_36, '36', 110)

    union_rdd_56 = spark.sparkContext.union([task_rdd_73])
    task_rdd_56 = simulate_task(union_rdd_56, '56', 6)

    union_rdd_100 = spark.sparkContext.union([task_rdd_98, task_rdd_99])
    task_rdd_100 = simulate_task(union_rdd_100, '100', 97)

    union_rdd_55 = spark.sparkContext.union([task_rdd_54, task_rdd_56])
    task_rdd_55 = simulate_task(union_rdd_55, '55', 52)

    union_rdd_49 = spark.sparkContext.union([task_rdd_100])
    task_rdd_49 = simulate_task(union_rdd_49, '49', 15)

    union_rdd_21 = spark.sparkContext.union([task_rdd_55])
    task_rdd_21 = simulate_task(union_rdd_21, '21', 13)

    union_rdd_50 = spark.sparkContext.union([task_rdd_49])
    task_rdd_50 = simulate_task(union_rdd_50, '50', 19)

    union_rdd_51 = spark.sparkContext.union([task_rdd_50])
    task_rdd_51 = simulate_task(union_rdd_51, '51', 24)

    union_rdd_4 = spark.sparkContext.union([task_rdd_51])
    task_rdd_4 = simulate_task(union_rdd_4, '4', 7)

    union_rdd_11 = spark.sparkContext.union([task_rdd_1, task_rdd_2, task_rdd_3, task_rdd_4, task_rdd_5, task_rdd_6, task_rdd_8, task_rdd_9, task_rdd_10])
    task_rdd_11 = simulate_task(union_rdd_11, '11', 34)

    union_rdd_40 = spark.sparkContext.union([task_rdd_2, task_rdd_4, task_rdd_8, task_rdd_14, task_rdd_16, task_rdd_18, task_rdd_20, task_rdd_22, task_rdd_24, task_rdd_39])
    task_rdd_40 = simulate_task(union_rdd_40, '40', 146)

    union_rdd_12 = spark.sparkContext.union([task_rdd_1, task_rdd_2, task_rdd_8, task_rdd_11])
    task_rdd_12 = simulate_task(union_rdd_12, '12', 28)

    union_rdd_41 = spark.sparkContext.union([task_rdd_16, task_rdd_18, task_rdd_40])
    task_rdd_41 = simulate_task(union_rdd_41, '41', 34)

    union_rdd_88 = spark.sparkContext.union([task_rdd_41])
    task_rdd_88 = simulate_task(union_rdd_88, '88', 5)

    union_rdd_61 = spark.sparkContext.union([task_rdd_88])
    task_rdd_61 = simulate_task(union_rdd_61, '61', 4)

    union_rdd_7 = spark.sparkContext.union([task_rdd_61])
    task_rdd_7 = simulate_task(union_rdd_7, '7', 10)

    combined_rdd = spark.sparkContext.union([task_rdd_4, task_rdd_8, task_rdd_12])
    task_rdd = simulate_task(combined_rdd, 'R13_4_8_12', 1)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
