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

    task_rdd_7 = simulate_task(base_rdd, '7', 28)
    task_rdd_14 = simulate_task(base_rdd, '14', 10)
    task_rdd_13 = simulate_task(base_rdd, '13', 62)
    task_rdd_12 = simulate_task(base_rdd, '12', 192)
    task_rdd_10 = simulate_task(base_rdd, '10', 1)
    task_rdd_3 = simulate_task(base_rdd, '3', 31)
    task_rdd_19 = simulate_task(base_rdd, '19', 17)
    task_rdd_9 = simulate_task(base_rdd, '9', 17)
    task_rdd_8 = simulate_task(base_rdd, '8', 17)
    task_rdd_5 = simulate_task(base_rdd, '5', 67)
    task_rdd_25 = simulate_task(base_rdd, '25', 254)
    task_rdd_4 = simulate_task(base_rdd, '4', 4)
    task_rdd_1 = simulate_task(base_rdd, '1', 1)
    task_rdd_81 = simulate_task(base_rdd, '81', 35)
    task_rdd_44 = simulate_task(base_rdd, '44', 84)
    task_rdd_37 = simulate_task(base_rdd, '37', 2)
    task_rdd_2 = simulate_task(base_rdd, '2', 18)
    task_rdd_34 = simulate_task(base_rdd, '34', 3)
    task_rdd_27 = simulate_task(base_rdd, '27', 36)
    task_rdd_31 = simulate_task(base_rdd, '31', 9)
    task_rdd_64 = simulate_task(base_rdd, '64', 1)
    task_rdd_29 = simulate_task(base_rdd, '29', 156)
    task_rdd_52 = simulate_task(base_rdd, '52', 77)

    union_rdd_11 = spark.sparkContext.union([task_rdd_52])
    task_rdd_11 = simulate_task(union_rdd_11, '11', 6)

    union_rdd_41 = spark.sparkContext.union([task_rdd_3])
    task_rdd_41 = simulate_task(union_rdd_41, '41', 62)

    union_rdd_20 = spark.sparkContext.union([task_rdd_8, task_rdd_19])
    task_rdd_20 = simulate_task(union_rdd_20, '20', 247)

    union_rdd_18 = spark.sparkContext.union([task_rdd_5, task_rdd_9])
    task_rdd_18 = simulate_task(union_rdd_18, '18', 300)

    union_rdd_26 = spark.sparkContext.union([task_rdd_4, task_rdd_25])
    task_rdd_26 = simulate_task(union_rdd_26, '26', 8)

    union_rdd_62 = spark.sparkContext.union([task_rdd_81])
    task_rdd_62 = simulate_task(union_rdd_62, '62', 2)

    union_rdd_47 = spark.sparkContext.union([task_rdd_44])
    task_rdd_47 = simulate_task(union_rdd_47, '47', 72)

    union_rdd_43 = spark.sparkContext.union([task_rdd_37])
    task_rdd_43 = simulate_task(union_rdd_43, '43', 43)

    union_rdd_35 = spark.sparkContext.union([task_rdd_34])
    task_rdd_35 = simulate_task(union_rdd_35, '35', 8)

    union_rdd_28 = spark.sparkContext.union([task_rdd_27])
    task_rdd_28 = simulate_task(union_rdd_28, '28', 398)

    union_rdd_39 = spark.sparkContext.union([task_rdd_29])
    task_rdd_39 = simulate_task(union_rdd_39, '39', 237)

    union_rdd_15 = spark.sparkContext.union([task_rdd_7, task_rdd_9, task_rdd_10, task_rdd_11, task_rdd_12, task_rdd_13, task_rdd_14])
    task_rdd_15 = simulate_task(union_rdd_15, '15', 4)

    union_rdd_32 = spark.sparkContext.union([task_rdd_41])
    task_rdd_32 = simulate_task(union_rdd_32, '32', 8)

    union_rdd_57 = spark.sparkContext.union([task_rdd_47])
    task_rdd_57 = simulate_task(union_rdd_57, '57', 191)

    union_rdd_36 = spark.sparkContext.union([task_rdd_27, task_rdd_28, task_rdd_35])
    task_rdd_36 = simulate_task(union_rdd_36, '36', 45)

    union_rdd_40 = spark.sparkContext.union([task_rdd_39])
    task_rdd_40 = simulate_task(union_rdd_40, '40', 61)

    union_rdd_16 = spark.sparkContext.union([task_rdd_9, task_rdd_13, task_rdd_15])
    task_rdd_16 = simulate_task(union_rdd_16, '16', 179)

    union_rdd_33 = spark.sparkContext.union([task_rdd_1, task_rdd_2, task_rdd_3, task_rdd_32])
    task_rdd_33 = simulate_task(union_rdd_33, '33', 561)

    union_rdd_87 = spark.sparkContext.union([task_rdd_44, task_rdd_57])
    task_rdd_87 = simulate_task(union_rdd_87, '87', 5)

    union_rdd_58 = spark.sparkContext.union([task_rdd_40])
    task_rdd_58 = simulate_task(union_rdd_58, '58', 4)

    union_rdd_17 = spark.sparkContext.union([task_rdd_11, task_rdd_13, task_rdd_14, task_rdd_16])
    task_rdd_17 = simulate_task(union_rdd_17, '17', 5)

    union_rdd_88 = spark.sparkContext.union([task_rdd_87])
    task_rdd_88 = simulate_task(union_rdd_88, '88', 63)

    union_rdd_59 = spark.sparkContext.union([task_rdd_58])
    task_rdd_59 = simulate_task(union_rdd_59, '59', 121)

    union_rdd_6 = spark.sparkContext.union([task_rdd_59])
    task_rdd_6 = simulate_task(union_rdd_6, '6', 4)

    union_rdd_61 = spark.sparkContext.union([task_rdd_88])
    task_rdd_61 = simulate_task(union_rdd_61, '61', 4)

    union_rdd_21 = spark.sparkContext.union([task_rdd_1, task_rdd_2, task_rdd_4, task_rdd_6, task_rdd_9, task_rdd_19, task_rdd_20])
    task_rdd_21 = simulate_task(union_rdd_21, '21', 155)

    union_rdd_63 = spark.sparkContext.union([task_rdd_61, task_rdd_62])
    task_rdd_63 = simulate_task(union_rdd_63, '63', 8)

    union_rdd_22 = spark.sparkContext.union([task_rdd_21])
    task_rdd_22 = simulate_task(union_rdd_22, '22', 60)

    union_rdd_38 = spark.sparkContext.union([task_rdd_63])
    task_rdd_38 = simulate_task(union_rdd_38, '38', 4)

    union_rdd_23 = spark.sparkContext.union([task_rdd_4, task_rdd_22])
    task_rdd_23 = simulate_task(union_rdd_23, '23', 63)

    union_rdd_65 = spark.sparkContext.union([task_rdd_38])
    task_rdd_65 = simulate_task(union_rdd_65, '65', 795)

    union_rdd_24 = spark.sparkContext.union([task_rdd_19, task_rdd_23])
    task_rdd_24 = simulate_task(union_rdd_24, '24', 18)

    union_rdd_66 = spark.sparkContext.union([task_rdd_31, task_rdd_65])
    task_rdd_66 = simulate_task(union_rdd_66, '66', 17)

    union_rdd_42 = spark.sparkContext.union([task_rdd_64, task_rdd_66])
    task_rdd_42 = simulate_task(union_rdd_42, '42', 51)

    combined_rdd = spark.sparkContext.union([task_rdd_17])
    task_rdd = simulate_task(combined_rdd, 'J7_17', 19)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
