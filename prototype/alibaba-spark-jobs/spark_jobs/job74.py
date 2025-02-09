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

    task_rdd_8 = simulate_task(base_rdd, '8', 5)
    task_rdd_30 = simulate_task(base_rdd, '30', 4)
    task_rdd_5 = simulate_task(base_rdd, '5', 35)
    task_rdd_7 = simulate_task(base_rdd, '7', 23)
    task_rdd_3 = simulate_task(base_rdd, '3', 17)
    task_rdd_42 = simulate_task(base_rdd, '42', 5)
    task_rdd_10 = simulate_task(base_rdd, '10', 27)
    task_rdd_9 = simulate_task(base_rdd, '9', 113)
    task_rdd_41 = simulate_task(base_rdd, '41', 1)
    task_rdd_38 = simulate_task(base_rdd, '38', 1)
    task_rdd_37 = simulate_task(base_rdd, '37', 1)
    task_rdd_36 = simulate_task(base_rdd, '36', 2)
    task_rdd_24 = simulate_task(base_rdd, '24', 5)
    task_rdd_21 = simulate_task(base_rdd, '21', 40)
    task_rdd_2 = simulate_task(base_rdd, '2', 60)
    task_rdd_1 = simulate_task(base_rdd, '1', 20)
    task_rdd_22 = simulate_task(base_rdd, '22', 13)
    task_rdd_11 = simulate_task(base_rdd, '11', 39)
    task_rdd_28 = simulate_task(base_rdd, '28', 2)
    task_rdd_131 = simulate_task(base_rdd, '131', 1)
    task_rdd_55 = simulate_task(base_rdd, '55', 2)
    task_rdd_65 = simulate_task(base_rdd, '65', 14)
    task_rdd_50 = simulate_task(base_rdd, '50', 14)

    union_rdd_15 = spark.sparkContext.union([task_rdd_8])
    task_rdd_15 = simulate_task(union_rdd_15, '15', 9)

    union_rdd_32 = spark.sparkContext.union([task_rdd_30])
    task_rdd_32 = simulate_task(union_rdd_32, '32', 96)

    union_rdd_40 = spark.sparkContext.union([task_rdd_5])
    task_rdd_40 = simulate_task(union_rdd_40, '40', 25)

    union_rdd_43 = spark.sparkContext.union([task_rdd_10, task_rdd_42])
    task_rdd_43 = simulate_task(union_rdd_43, '43', 17)

    union_rdd_39 = spark.sparkContext.union([task_rdd_38])
    task_rdd_39 = simulate_task(union_rdd_39, '39', 126)

    union_rdd_25 = spark.sparkContext.union([task_rdd_24])
    task_rdd_25 = simulate_task(union_rdd_25, '25', 9)

    union_rdd_23 = spark.sparkContext.union([task_rdd_22])
    task_rdd_23 = simulate_task(union_rdd_23, '23', 49)

    union_rdd_132 = spark.sparkContext.union([task_rdd_131])
    task_rdd_132 = simulate_task(union_rdd_132, '132', 1)

    union_rdd_48 = spark.sparkContext.union([task_rdd_50])
    task_rdd_48 = simulate_task(union_rdd_48, '48', 4)

    union_rdd_19 = spark.sparkContext.union([task_rdd_7, task_rdd_10, task_rdd_15])
    task_rdd_19 = simulate_task(union_rdd_19, '19', 25)

    union_rdd_44 = spark.sparkContext.union([task_rdd_10, task_rdd_21, task_rdd_39, task_rdd_41, task_rdd_43])
    task_rdd_44 = simulate_task(union_rdd_44, '44', 69)

    union_rdd_29 = spark.sparkContext.union([task_rdd_11, task_rdd_23, task_rdd_2])
    task_rdd_29 = simulate_task(union_rdd_29, '29', 6)

    union_rdd_66 = spark.sparkContext.union([task_rdd_48, task_rdd_65])
    task_rdd_66 = simulate_task(union_rdd_66, '66', 291)

    union_rdd_6 = spark.sparkContext.union([task_rdd_66])
    task_rdd_6 = simulate_task(union_rdd_6, '6', 1106)

    union_rdd_45 = spark.sparkContext.union([task_rdd_22, task_rdd_24, task_rdd_28, task_rdd_36, task_rdd_37, task_rdd_44])
    task_rdd_45 = simulate_task(union_rdd_45, '45', 191)

    union_rdd_20 = spark.sparkContext.union([task_rdd_1, task_rdd_2, task_rdd_5, task_rdd_6, task_rdd_7, task_rdd_8, task_rdd_9, task_rdd_19])
    task_rdd_20 = simulate_task(union_rdd_20, '20', 252)

    union_rdd_33 = spark.sparkContext.union([task_rdd_6, task_rdd_22, task_rdd_32])
    task_rdd_33 = simulate_task(union_rdd_33, '33', 136)

    union_rdd_46 = spark.sparkContext.union([task_rdd_45])
    task_rdd_46 = simulate_task(union_rdd_46, '46', 26)

    union_rdd_31 = spark.sparkContext.union([task_rdd_20])
    task_rdd_31 = simulate_task(union_rdd_31, '31', 14)

    union_rdd_34 = spark.sparkContext.union([task_rdd_33])
    task_rdd_34 = simulate_task(union_rdd_34, '34', 675)

    union_rdd_18 = spark.sparkContext.union([task_rdd_10, task_rdd_11])
    task_rdd_18 = simulate_task(union_rdd_18, '18', 40)

    union_rdd_26 = spark.sparkContext.union([task_rdd_30])
    task_rdd_26 = simulate_task(union_rdd_26, '26', 383)

    union_rdd_12 = spark.sparkContext.union([task_rdd_11, task_rdd_18])
    task_rdd_12 = simulate_task(union_rdd_12, '12', 39)

    union_rdd_27 = spark.sparkContext.union([task_rdd_5, task_rdd_9, task_rdd_26])
    task_rdd_27 = simulate_task(union_rdd_27, '27', 868)

    union_rdd_13 = spark.sparkContext.union([task_rdd_2, task_rdd_6, task_rdd_12])
    task_rdd_13 = simulate_task(union_rdd_13, '13', 102)

    union_rdd_16 = spark.sparkContext.union([task_rdd_24, task_rdd_27])
    task_rdd_16 = simulate_task(union_rdd_16, '16', 87)

    union_rdd_14 = spark.sparkContext.union([task_rdd_3, task_rdd_6, task_rdd_10, task_rdd_13])
    task_rdd_14 = simulate_task(union_rdd_14, '14', 3)

    union_rdd_4 = spark.sparkContext.union([task_rdd_3, task_rdd_16])
    task_rdd_4 = simulate_task(union_rdd_4, '4', 309)

    combined_rdd = spark.sparkContext.union([task_rdd_9, task_rdd_11, task_rdd_14])
    task_rdd = simulate_task(combined_rdd, 'J17_9_11_14', 36)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
