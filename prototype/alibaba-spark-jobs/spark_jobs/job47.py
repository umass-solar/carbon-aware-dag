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

    task_rdd_3 = simulate_task(base_rdd, '3', 87)
    task_rdd_17 = simulate_task(base_rdd, '17', 59)
    task_rdd_13 = simulate_task(base_rdd, '13', 22)
    task_rdd_9 = simulate_task(base_rdd, '9', 17)
    task_rdd_8 = simulate_task(base_rdd, '8', 7)
    task_rdd_7 = simulate_task(base_rdd, '7', 156)
    task_rdd_6 = simulate_task(base_rdd, '6', 34)
    task_rdd_5 = simulate_task(base_rdd, '5', 3)
    task_rdd_1 = simulate_task(base_rdd, '1', 9)
    task_rdd_4 = simulate_task(base_rdd, '4', 162)
    task_rdd_2 = simulate_task(base_rdd, '2', 6)
    task_rdd_29 = simulate_task(base_rdd, '29', 62)
    task_rdd_132 = simulate_task(base_rdd, '132', 4)
    task_rdd_50 = simulate_task(base_rdd, '50', 1)
    task_rdd_24 = simulate_task(base_rdd, '24', 4)
    task_rdd_23 = simulate_task(base_rdd, '23', 5)
    task_rdd_16 = simulate_task(base_rdd, '16', 10)
    task_rdd_44 = simulate_task(base_rdd, '44', 28)
    task_rdd_26 = simulate_task(base_rdd, '26', 30)
    task_rdd_33 = simulate_task(base_rdd, '33', 11)
    task_rdd_79 = simulate_task(base_rdd, '79', 4)
    task_rdd_47 = simulate_task(base_rdd, '47', 34)
    task_rdd_37 = simulate_task(base_rdd, '37', 66)
    task_rdd_25 = simulate_task(base_rdd, '25', 87)
    task_rdd_40 = simulate_task(base_rdd, '40', 157)
    task_rdd_39 = simulate_task(base_rdd, '39', 35)

    union_rdd_18 = spark.sparkContext.union([task_rdd_1, task_rdd_2, task_rdd_3, task_rdd_4, task_rdd_5, task_rdd_6, task_rdd_7, task_rdd_17])
    task_rdd_18 = simulate_task(union_rdd_18, '18', 2655)

    union_rdd_66 = spark.sparkContext.union([task_rdd_13])
    task_rdd_66 = simulate_task(union_rdd_66, '66', 141)

    union_rdd_30 = spark.sparkContext.union([task_rdd_29])
    task_rdd_30 = simulate_task(union_rdd_30, '30', 63)

    union_rdd_89 = spark.sparkContext.union([task_rdd_132])
    task_rdd_89 = simulate_task(union_rdd_89, '89', 4)

    union_rdd_46 = spark.sparkContext.union([task_rdd_24])
    task_rdd_46 = simulate_task(union_rdd_46, '46', 15)

    union_rdd_41 = spark.sparkContext.union([task_rdd_44])
    task_rdd_41 = simulate_task(union_rdd_41, '41', 207)

    union_rdd_59 = spark.sparkContext.union([task_rdd_79])
    task_rdd_59 = simulate_task(union_rdd_59, '59', 319)

    union_rdd_38 = spark.sparkContext.union([task_rdd_37])
    task_rdd_38 = simulate_task(union_rdd_38, '38', 99)

    union_rdd_34 = spark.sparkContext.union([task_rdd_25])
    task_rdd_34 = simulate_task(union_rdd_34, '34', 23)

    union_rdd_61 = spark.sparkContext.union([task_rdd_39, task_rdd_40])
    task_rdd_61 = simulate_task(union_rdd_61, '61', 27)

    union_rdd_14 = spark.sparkContext.union([task_rdd_17, task_rdd_18])
    task_rdd_14 = simulate_task(union_rdd_14, '14', 12)

    union_rdd_119 = spark.sparkContext.union([task_rdd_30])
    task_rdd_119 = simulate_task(union_rdd_119, '119', 6)

    union_rdd_51 = spark.sparkContext.union([task_rdd_46, task_rdd_50])
    task_rdd_51 = simulate_task(union_rdd_51, '51', 52)

    union_rdd_42 = spark.sparkContext.union([task_rdd_26, task_rdd_41])
    task_rdd_42 = simulate_task(union_rdd_42, '42', 216)

    union_rdd_35 = spark.sparkContext.union([task_rdd_34])
    task_rdd_35 = simulate_task(union_rdd_35, '35', 13)

    union_rdd_15 = spark.sparkContext.union([task_rdd_5, task_rdd_6, task_rdd_14])
    task_rdd_15 = simulate_task(union_rdd_15, '15', 1760)

    union_rdd_120 = spark.sparkContext.union([task_rdd_89, task_rdd_119])
    task_rdd_120 = simulate_task(union_rdd_120, '120', 275)

    union_rdd_43 = spark.sparkContext.union([task_rdd_42])
    task_rdd_43 = simulate_task(union_rdd_43, '43', 42)

    union_rdd_36 = spark.sparkContext.union([task_rdd_35])
    task_rdd_36 = simulate_task(union_rdd_36, '36', 7)

    union_rdd_10 = spark.sparkContext.union([task_rdd_3, task_rdd_9, task_rdd_15])
    task_rdd_10 = simulate_task(union_rdd_10, '10', 10)

    union_rdd_52 = spark.sparkContext.union([task_rdd_120])
    task_rdd_52 = simulate_task(union_rdd_52, '52', 4)

    union_rdd_45 = spark.sparkContext.union([task_rdd_36])
    task_rdd_45 = simulate_task(union_rdd_45, '45', 4)

    union_rdd_11 = spark.sparkContext.union([task_rdd_7, task_rdd_8, task_rdd_10])
    task_rdd_11 = simulate_task(union_rdd_11, '11', 171)

    union_rdd_31 = spark.sparkContext.union([task_rdd_51, task_rdd_52])
    task_rdd_31 = simulate_task(union_rdd_31, '31', 3)

    union_rdd_48 = spark.sparkContext.union([task_rdd_45, task_rdd_47])
    task_rdd_48 = simulate_task(union_rdd_48, '48', 2)

    union_rdd_12 = spark.sparkContext.union([task_rdd_2, task_rdd_3, task_rdd_5, task_rdd_7, task_rdd_11])
    task_rdd_12 = simulate_task(union_rdd_12, '12', 4)

    union_rdd_32 = spark.sparkContext.union([task_rdd_31])
    task_rdd_32 = simulate_task(union_rdd_32, '32', 4)

    union_rdd_49 = spark.sparkContext.union([task_rdd_45, task_rdd_47, task_rdd_48])
    task_rdd_49 = simulate_task(union_rdd_49, '49', 16)

    union_rdd_60 = spark.sparkContext.union([task_rdd_49, task_rdd_59])
    task_rdd_60 = simulate_task(union_rdd_60, '60', 8)

    union_rdd_63 = spark.sparkContext.union([task_rdd_44, task_rdd_60, task_rdd_66])
    task_rdd_63 = simulate_task(union_rdd_63, '63', 21)

    union_rdd_64 = spark.sparkContext.union([task_rdd_63])
    task_rdd_64 = simulate_task(union_rdd_64, '64', 2)

    union_rdd_62 = spark.sparkContext.union([task_rdd_61, task_rdd_64])
    task_rdd_62 = simulate_task(union_rdd_62, '62', 83)

    union_rdd_27 = spark.sparkContext.union([task_rdd_24, task_rdd_26, task_rdd_62])
    task_rdd_27 = simulate_task(union_rdd_27, '27', 98)

    union_rdd_28 = spark.sparkContext.union([task_rdd_23, task_rdd_26, task_rdd_27])
    task_rdd_28 = simulate_task(union_rdd_28, '28', 7)

    union_rdd_22 = spark.sparkContext.union([task_rdd_28])
    task_rdd_22 = simulate_task(union_rdd_22, '22', 11)

    union_rdd_19 = spark.sparkContext.union([task_rdd_2, task_rdd_8, task_rdd_22])
    task_rdd_19 = simulate_task(union_rdd_19, '19', 6)

    union_rdd_20 = spark.sparkContext.union([task_rdd_13, task_rdd_19])
    task_rdd_20 = simulate_task(union_rdd_20, '20', 32)

    union_rdd_21 = spark.sparkContext.union([task_rdd_13, task_rdd_20])
    task_rdd_21 = simulate_task(union_rdd_21, '21', 13)

    combined_rdd = spark.sparkContext.union([task_rdd_1, task_rdd_2, task_rdd_12])
    task_rdd = simulate_task(combined_rdd, 'J3_1_2_12', 39)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
