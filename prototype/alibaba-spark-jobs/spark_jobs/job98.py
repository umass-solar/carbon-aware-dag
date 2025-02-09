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

    task_rdd_8 = simulate_task(base_rdd, '8', 54)
    task_rdd_16 = simulate_task(base_rdd, '16', 21)
    task_rdd_35 = simulate_task(base_rdd, '35', 84)
    task_rdd_13 = simulate_task(base_rdd, '13', 51)
    task_rdd_4 = simulate_task(base_rdd, '4', 90)
    task_rdd_1 = simulate_task(base_rdd, '1', 25)
    task_rdd_6 = simulate_task(base_rdd, '6', 70)
    task_rdd_5 = simulate_task(base_rdd, '5', 66)
    task_rdd_3 = simulate_task(base_rdd, '3', 20)
    task_rdd_18 = simulate_task(base_rdd, '18', 149)
    task_rdd_14 = simulate_task(base_rdd, '14', 72)
    task_rdd_7 = simulate_task(base_rdd, '7', 17)
    task_rdd_132 = simulate_task(base_rdd, '132', 4)
    task_rdd_27 = simulate_task(base_rdd, '27', 2)
    task_rdd_25 = simulate_task(base_rdd, '25', 13)
    task_rdd_23 = simulate_task(base_rdd, '23', 53)
    task_rdd_21 = simulate_task(base_rdd, '21', 8)
    task_rdd_45 = simulate_task(base_rdd, '45', 72)
    task_rdd_49 = simulate_task(base_rdd, '49', 27)
    task_rdd_32 = simulate_task(base_rdd, '32', 29)
    task_rdd_31 = simulate_task(base_rdd, '31', 2)
    task_rdd_24 = simulate_task(base_rdd, '24', 36)
    task_rdd_51 = simulate_task(base_rdd, '51', 32)
    task_rdd_58 = simulate_task(base_rdd, '58', 81)

    union_rdd_9 = spark.sparkContext.union([task_rdd_1, task_rdd_7, task_rdd_8])
    task_rdd_9 = simulate_task(union_rdd_9, '9', 37)

    union_rdd_17 = spark.sparkContext.union([task_rdd_7, task_rdd_16])
    task_rdd_17 = simulate_task(union_rdd_17, '17', 8)

    union_rdd_36 = spark.sparkContext.union([task_rdd_35])
    task_rdd_36 = simulate_task(union_rdd_36, '36', 159)

    union_rdd_48 = spark.sparkContext.union([task_rdd_13])
    task_rdd_48 = simulate_task(union_rdd_48, '48', 141)

    union_rdd_11 = spark.sparkContext.union([task_rdd_1, task_rdd_4])
    task_rdd_11 = simulate_task(union_rdd_11, '11', 1)

    union_rdd_19 = spark.sparkContext.union([task_rdd_18])
    task_rdd_19 = simulate_task(union_rdd_19, '19', 399)

    union_rdd_38 = spark.sparkContext.union([task_rdd_24, task_rdd_32])
    task_rdd_38 = simulate_task(union_rdd_38, '38', 6)

    union_rdd_120 = spark.sparkContext.union([task_rdd_132])
    task_rdd_120 = simulate_task(union_rdd_120, '120', 4)

    union_rdd_28 = spark.sparkContext.union([task_rdd_27])
    task_rdd_28 = simulate_task(union_rdd_28, '28', 729)

    union_rdd_41 = spark.sparkContext.union([task_rdd_58])
    task_rdd_41 = simulate_task(union_rdd_41, '41', 4)

    union_rdd_22 = spark.sparkContext.union([task_rdd_49])
    task_rdd_22 = simulate_task(union_rdd_22, '22', 113)

    union_rdd_10 = spark.sparkContext.union([task_rdd_1, task_rdd_3, task_rdd_5, task_rdd_6, task_rdd_7, task_rdd_8, task_rdd_9])
    task_rdd_10 = simulate_task(union_rdd_10, '10', 23)

    union_rdd_37 = spark.sparkContext.union([task_rdd_36])
    task_rdd_37 = simulate_task(union_rdd_37, '37', 1874)

    union_rdd_33 = spark.sparkContext.union([task_rdd_48])
    task_rdd_33 = simulate_task(union_rdd_33, '33', 26)

    union_rdd_20 = spark.sparkContext.union([task_rdd_14, task_rdd_19])
    task_rdd_20 = simulate_task(union_rdd_20, '20', 8)

    union_rdd_73 = spark.sparkContext.union([task_rdd_120])
    task_rdd_73 = simulate_task(union_rdd_73, '73', 4)

    union_rdd_29 = spark.sparkContext.union([task_rdd_25, task_rdd_28])
    task_rdd_29 = simulate_task(union_rdd_29, '29', 51)

    union_rdd_42 = spark.sparkContext.union([task_rdd_23, task_rdd_41])
    task_rdd_42 = simulate_task(union_rdd_42, '42', 16)

    union_rdd_12 = spark.sparkContext.union([task_rdd_10])
    task_rdd_12 = simulate_task(union_rdd_12, '12', 414)

    union_rdd_26 = spark.sparkContext.union([task_rdd_35, task_rdd_37])
    task_rdd_26 = simulate_task(union_rdd_26, '26', 9)

    union_rdd_34 = spark.sparkContext.union([task_rdd_6, task_rdd_31, task_rdd_32, task_rdd_33])
    task_rdd_34 = simulate_task(union_rdd_34, '34', 4)

    union_rdd_39 = spark.sparkContext.union([task_rdd_22, task_rdd_25, task_rdd_27, task_rdd_29, task_rdd_38])
    task_rdd_39 = simulate_task(union_rdd_39, '39', 19)

    union_rdd_43 = spark.sparkContext.union([task_rdd_42])
    task_rdd_43 = simulate_task(union_rdd_43, '43', 44)

    union_rdd_2 = spark.sparkContext.union([task_rdd_6, task_rdd_12])
    task_rdd_2 = simulate_task(union_rdd_2, '2', 195)

    union_rdd_15 = spark.sparkContext.union([task_rdd_14, task_rdd_21, task_rdd_23, task_rdd_26])
    task_rdd_15 = simulate_task(union_rdd_15, '15', 7)

    union_rdd_40 = spark.sparkContext.union([task_rdd_38, task_rdd_39])
    task_rdd_40 = simulate_task(union_rdd_40, '40', 3)

    union_rdd_44 = spark.sparkContext.union([task_rdd_41, task_rdd_43])
    task_rdd_44 = simulate_task(union_rdd_44, '44', 198)

    union_rdd_30 = spark.sparkContext.union([task_rdd_2])
    task_rdd_30 = simulate_task(union_rdd_30, '30', 10)

    union_rdd_88 = spark.sparkContext.union([task_rdd_30])
    task_rdd_88 = simulate_task(union_rdd_88, '88', 5)

    combined_rdd = spark.sparkContext.union([task_rdd_32, task_rdd_41, task_rdd_51, task_rdd_88])
    task_rdd = simulate_task(combined_rdd, 'R89_32_41_51_88', 20)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
