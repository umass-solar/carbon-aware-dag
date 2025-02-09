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

    task_rdd_19 = simulate_task(base_rdd, '19', 2)
    task_rdd_5 = simulate_task(base_rdd, '5', 28)
    task_rdd_2 = simulate_task(base_rdd, '2', 2)
    task_rdd_10 = simulate_task(base_rdd, '10', 45)
    task_rdd_9 = simulate_task(base_rdd, '9', 11)
    task_rdd_8 = simulate_task(base_rdd, '8', 3)
    task_rdd_16 = simulate_task(base_rdd, '16', 22)
    task_rdd_1 = simulate_task(base_rdd, '1', 148)
    task_rdd_7 = simulate_task(base_rdd, '7', 91)
    task_rdd_4 = simulate_task(base_rdd, '4', 3)
    task_rdd_26 = simulate_task(base_rdd, '26', 12)
    task_rdd_25 = simulate_task(base_rdd, '25', 13)
    task_rdd_17 = simulate_task(base_rdd, '17', 197)
    task_rdd_3 = simulate_task(base_rdd, '3', 27)
    task_rdd_32 = simulate_task(base_rdd, '32', 40)
    task_rdd_66 = simulate_task(base_rdd, '66', 4)
    task_rdd_35 = simulate_task(base_rdd, '35', 4)
    task_rdd_22 = simulate_task(base_rdd, '22', 36)
    task_rdd_21 = simulate_task(base_rdd, '21', 35)
    task_rdd_30 = simulate_task(base_rdd, '30', 199)
    task_rdd_29 = simulate_task(base_rdd, '29', 2)
    task_rdd_46 = simulate_task(base_rdd, '46', 11)
    task_rdd_37 = simulate_task(base_rdd, '37', 6)
    task_rdd_48 = simulate_task(base_rdd, '48', 2)

    union_rdd_6 = spark.sparkContext.union([task_rdd_2, task_rdd_4, task_rdd_5])
    task_rdd_6 = simulate_task(union_rdd_6, '6', 1)

    union_rdd_11 = spark.sparkContext.union([task_rdd_1, task_rdd_3, task_rdd_4, task_rdd_9, task_rdd_10])
    task_rdd_11 = simulate_task(union_rdd_11, '11', 4)

    union_rdd_18 = spark.sparkContext.union([task_rdd_17])
    task_rdd_18 = simulate_task(union_rdd_18, '18', 5)

    union_rdd_14 = spark.sparkContext.union([task_rdd_3])
    task_rdd_14 = simulate_task(union_rdd_14, '14', 8)

    union_rdd_33 = spark.sparkContext.union([task_rdd_32])
    task_rdd_33 = simulate_task(union_rdd_33, '33', 196)

    union_rdd_73 = spark.sparkContext.union([task_rdd_66])
    task_rdd_73 = simulate_task(union_rdd_73, '73', 72)

    union_rdd_36 = spark.sparkContext.union([task_rdd_35])
    task_rdd_36 = simulate_task(union_rdd_36, '36', 11)

    union_rdd_31 = spark.sparkContext.union([task_rdd_30])
    task_rdd_31 = simulate_task(union_rdd_31, '31', 25)

    union_rdd_47 = spark.sparkContext.union([task_rdd_37, task_rdd_46])
    task_rdd_47 = simulate_task(union_rdd_47, '47', 307)

    union_rdd_12 = spark.sparkContext.union([task_rdd_4, task_rdd_8, task_rdd_11])
    task_rdd_12 = simulate_task(union_rdd_12, '12', 33)

    union_rdd_15 = spark.sparkContext.union([task_rdd_14])
    task_rdd_15 = simulate_task(union_rdd_15, '15', 169)

    union_rdd_34 = spark.sparkContext.union([task_rdd_33])
    task_rdd_34 = simulate_task(union_rdd_34, '34', 5)

    union_rdd_41 = spark.sparkContext.union([task_rdd_73])
    task_rdd_41 = simulate_task(union_rdd_41, '41', 3)

    union_rdd_40 = spark.sparkContext.union([task_rdd_30, task_rdd_31, task_rdd_35, task_rdd_36])
    task_rdd_40 = simulate_task(union_rdd_40, '40', 32)

    union_rdd_50 = spark.sparkContext.union([task_rdd_47])
    task_rdd_50 = simulate_task(union_rdd_50, '50', 162)

    union_rdd_44 = spark.sparkContext.union([task_rdd_25, task_rdd_26])
    task_rdd_44 = simulate_task(union_rdd_44, '44', 3)

    union_rdd_52 = spark.sparkContext.union([])
    task_rdd_52 = simulate_task(union_rdd_52, '52', 4)

    union_rdd_45 = spark.sparkContext.union([task_rdd_40, task_rdd_41, task_rdd_44])
    task_rdd_45 = simulate_task(union_rdd_45, '45', 6)

    union_rdd_53 = spark.sparkContext.union([task_rdd_50, task_rdd_52])
    task_rdd_53 = simulate_task(union_rdd_53, '53', 3)

    union_rdd_43 = spark.sparkContext.union([task_rdd_45])
    task_rdd_43 = simulate_task(union_rdd_43, '43', 2)

    union_rdd_54 = spark.sparkContext.union([task_rdd_53])
    task_rdd_54 = simulate_task(union_rdd_54, '54', 76)

    union_rdd_39 = spark.sparkContext.union([task_rdd_43])
    task_rdd_39 = simulate_task(union_rdd_39, '39', 15)

    union_rdd_61 = spark.sparkContext.union([task_rdd_54])
    task_rdd_61 = simulate_task(union_rdd_61, '61', 243)

    union_rdd_27 = spark.sparkContext.union([task_rdd_39])
    task_rdd_27 = simulate_task(union_rdd_27, '27', 35)

    union_rdd_28 = spark.sparkContext.union([task_rdd_27])
    task_rdd_28 = simulate_task(union_rdd_28, '28', 97)

    union_rdd_23 = spark.sparkContext.union([task_rdd_22, task_rdd_25, task_rdd_28])
    task_rdd_23 = simulate_task(union_rdd_23, '23', 10)

    union_rdd_24 = spark.sparkContext.union([task_rdd_3, task_rdd_23])
    task_rdd_24 = simulate_task(union_rdd_24, '24', 17)

    union_rdd_56 = spark.sparkContext.union([task_rdd_24])
    task_rdd_56 = simulate_task(union_rdd_56, '56', 1027)

    combined_rdd = spark.sparkContext.union([task_rdd_3, task_rdd_5, task_rdd_6, task_rdd_19])
    task_rdd = simulate_task(combined_rdd, 'J20_3_5_6_19', 29)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
