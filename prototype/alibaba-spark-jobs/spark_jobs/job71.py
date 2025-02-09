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

    task_rdd_12 = simulate_task(base_rdd, '12', 51)
    task_rdd_131 = simulate_task(base_rdd, '131', 1)
    task_rdd_29 = simulate_task(base_rdd, '29', 12)
    task_rdd_20 = simulate_task(base_rdd, '20', 10)
    task_rdd_26 = simulate_task(base_rdd, '26', 12)
    task_rdd_3 = simulate_task(base_rdd, '3', 66)
    task_rdd_11 = simulate_task(base_rdd, '11', 11)
    task_rdd_5 = simulate_task(base_rdd, '5', 43)
    task_rdd_14 = simulate_task(base_rdd, '14', 55)
    task_rdd_4 = simulate_task(base_rdd, '4', 4)
    task_rdd_8 = simulate_task(base_rdd, '8', 114)
    task_rdd_18 = simulate_task(base_rdd, '18', 26)
    task_rdd_31 = simulate_task(base_rdd, '31', 56)
    task_rdd_19 = simulate_task(base_rdd, '19', 4)
    task_rdd_16 = simulate_task(base_rdd, '16', 251)
    task_rdd_24 = simulate_task(base_rdd, '24', 13)
    task_rdd_55 = simulate_task(base_rdd, '55', 32)
    task_rdd_57 = simulate_task(base_rdd, '57', 187)
    task_rdd_96 = simulate_task(base_rdd, '96', 4)
    task_rdd_95 = simulate_task(base_rdd, '95', 4)
    task_rdd_63 = simulate_task(base_rdd, '63', 5)
    task_rdd_44 = simulate_task(base_rdd, '44', 13)

    union_rdd_132 = spark.sparkContext.union([task_rdd_131])
    task_rdd_132 = simulate_task(union_rdd_132, '132', 1)

    union_rdd_42 = spark.sparkContext.union([task_rdd_20])
    task_rdd_42 = simulate_task(union_rdd_42, '42', 10)

    union_rdd_34 = spark.sparkContext.union([task_rdd_3, task_rdd_5, task_rdd_8, task_rdd_11, task_rdd_12, task_rdd_24, task_rdd_3])
    task_rdd_34 = simulate_task(union_rdd_34, '34', 39)

    union_rdd_64 = spark.sparkContext.union([task_rdd_5])
    task_rdd_64 = simulate_task(union_rdd_64, '64', 35)

    union_rdd_13 = spark.sparkContext.union([task_rdd_8, task_rdd_12, task_rdd_14])
    task_rdd_13 = simulate_task(union_rdd_13, '13', 11)

    union_rdd_1 = spark.sparkContext.union([task_rdd_63])
    task_rdd_1 = simulate_task(union_rdd_1, '1', 243)

    union_rdd_59 = spark.sparkContext.union([task_rdd_57])
    task_rdd_59 = simulate_task(union_rdd_59, '59', 3)

    union_rdd_97 = spark.sparkContext.union([task_rdd_95, task_rdd_96])
    task_rdd_97 = simulate_task(union_rdd_97, '97', 97)

    union_rdd_45 = spark.sparkContext.union([task_rdd_44])
    task_rdd_45 = simulate_task(union_rdd_45, '45', 54)

    union_rdd_58 = spark.sparkContext.union([task_rdd_132])
    task_rdd_58 = simulate_task(union_rdd_58, '58', 4)

    union_rdd_15 = spark.sparkContext.union([task_rdd_45])
    task_rdd_15 = simulate_task(union_rdd_15, '15', 30)

    union_rdd_25 = spark.sparkContext.union([task_rdd_42])
    task_rdd_25 = simulate_task(union_rdd_25, '25', 87)

    union_rdd_37 = spark.sparkContext.union([task_rdd_64])
    task_rdd_37 = simulate_task(union_rdd_37, '37', 84)

    union_rdd_52 = spark.sparkContext.union([task_rdd_59])
    task_rdd_52 = simulate_task(union_rdd_52, '52', 6)

    union_rdd_39 = spark.sparkContext.union([task_rdd_97])
    task_rdd_39 = simulate_task(union_rdd_39, '39', 3)

    union_rdd_41 = spark.sparkContext.union([task_rdd_58])
    task_rdd_41 = simulate_task(union_rdd_41, '41', 159)

    union_rdd_6 = spark.sparkContext.union([task_rdd_15])
    task_rdd_6 = simulate_task(union_rdd_6, '6', 8)

    union_rdd_30 = spark.sparkContext.union([task_rdd_25, task_rdd_29])
    task_rdd_30 = simulate_task(union_rdd_30, '30', 4)

    union_rdd_40 = spark.sparkContext.union([task_rdd_39])
    task_rdd_40 = simulate_task(union_rdd_40, '40', 12)

    union_rdd_9 = spark.sparkContext.union([task_rdd_41])
    task_rdd_9 = simulate_task(union_rdd_9, '9', 1168)

    union_rdd_53 = spark.sparkContext.union([task_rdd_40, task_rdd_52])
    task_rdd_53 = simulate_task(union_rdd_53, '53', 148)

    union_rdd_10 = spark.sparkContext.union([task_rdd_3, task_rdd_4, task_rdd_5, task_rdd_6, task_rdd_9])
    task_rdd_10 = simulate_task(union_rdd_10, '10', 8)

    union_rdd_56 = spark.sparkContext.union([task_rdd_53, task_rdd_55])
    task_rdd_56 = simulate_task(union_rdd_56, '56', 38)

    union_rdd_7 = spark.sparkContext.union([task_rdd_10])
    task_rdd_7 = simulate_task(union_rdd_7, '7', 58)

    union_rdd_32 = spark.sparkContext.union([task_rdd_56])
    task_rdd_32 = simulate_task(union_rdd_32, '32', 379)

    union_rdd_2 = spark.sparkContext.union([task_rdd_3, task_rdd_7])
    task_rdd_2 = simulate_task(union_rdd_2, '2', 247)

    union_rdd_35 = spark.sparkContext.union([task_rdd_32, task_rdd_34])
    task_rdd_35 = simulate_task(union_rdd_35, '35', 50)

    union_rdd_33 = spark.sparkContext.union([task_rdd_32])
    task_rdd_33 = simulate_task(union_rdd_33, '33', 1)

    union_rdd_47 = spark.sparkContext.union([task_rdd_2])
    task_rdd_47 = simulate_task(union_rdd_47, '47', 116)

    union_rdd_36 = spark.sparkContext.union([task_rdd_35])
    task_rdd_36 = simulate_task(union_rdd_36, '36', 11)

    union_rdd_38 = spark.sparkContext.union([task_rdd_14, task_rdd_19, task_rdd_33, task_rdd_37])
    task_rdd_38 = simulate_task(union_rdd_38, '38', 4)

    union_rdd_48 = spark.sparkContext.union([task_rdd_1, task_rdd_47])
    task_rdd_48 = simulate_task(union_rdd_48, '48', 16)

    union_rdd_27 = spark.sparkContext.union([task_rdd_24, task_rdd_48])
    task_rdd_27 = simulate_task(union_rdd_27, '27', 6)

    union_rdd_28 = spark.sparkContext.union([task_rdd_26, task_rdd_27])
    task_rdd_28 = simulate_task(union_rdd_28, '28', 1)

    union_rdd_22 = spark.sparkContext.union([task_rdd_12, task_rdd_24, task_rdd_28])
    task_rdd_22 = simulate_task(union_rdd_22, '22', 4)

    union_rdd_23 = spark.sparkContext.union([task_rdd_12, task_rdd_15, task_rdd_19, task_rdd_22])
    task_rdd_23 = simulate_task(union_rdd_23, '23', 2)

    union_rdd_67 = spark.sparkContext.union([task_rdd_23])
    task_rdd_67 = simulate_task(union_rdd_67, '67', 14)

    union_rdd_54 = spark.sparkContext.union([task_rdd_67])
    task_rdd_54 = simulate_task(union_rdd_54, '54', 5)

    union_rdd_17 = spark.sparkContext.union([task_rdd_18, task_rdd_54])
    task_rdd_17 = simulate_task(union_rdd_17, '17', 3)

    combined_rdd = spark.sparkContext.union([task_rdd_11, task_rdd_17])
    task_rdd = simulate_task(combined_rdd, 'J12_11_17', 17)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
