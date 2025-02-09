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

    task_rdd_12 = simulate_task(base_rdd, '12', 58)
    task_rdd_64 = simulate_task(base_rdd, '64', 11)
    task_rdd_8 = simulate_task(base_rdd, '8', 6)
    task_rdd_7 = simulate_task(base_rdd, '7', 17)
    task_rdd_11 = simulate_task(base_rdd, '11', 39)
    task_rdd_4 = simulate_task(base_rdd, '4', 12)
    task_rdd_1 = simulate_task(base_rdd, '1', 6)
    task_rdd_16 = simulate_task(base_rdd, '16', 97)
    task_rdd_18 = simulate_task(base_rdd, '18', 143)
    task_rdd_6 = simulate_task(base_rdd, '6', 28)
    task_rdd_5 = simulate_task(base_rdd, '5', 5)
    task_rdd_54 = simulate_task(base_rdd, '54', 97)
    task_rdd_53 = simulate_task(base_rdd, '53', 12)
    task_rdd_34 = simulate_task(base_rdd, '34', 8)
    task_rdd_40 = simulate_task(base_rdd, '40', 11)
    task_rdd_39 = simulate_task(base_rdd, '39', 22)
    task_rdd_27 = simulate_task(base_rdd, '27', 6)
    task_rdd_2 = simulate_task(base_rdd, '2', 3)
    task_rdd_38 = simulate_task(base_rdd, '38', 16)
    task_rdd_36 = simulate_task(base_rdd, '36', 17)
    task_rdd_26 = simulate_task(base_rdd, '26', 9)
    task_rdd_3 = simulate_task(base_rdd, '3', 19)
    task_rdd_33 = simulate_task(base_rdd, '33', 3)
    task_rdd_48 = simulate_task(base_rdd, '48', 162)
    task_rdd_49 = simulate_task(base_rdd, '49', 50)
    task_rdd_60 = simulate_task(base_rdd, '60', 2)
    task_rdd_42 = simulate_task(base_rdd, '42', 20)
    task_rdd_51 = simulate_task(base_rdd, '51', 8)
    task_rdd_24 = simulate_task(base_rdd, '24', 3)
    task_rdd_62 = simulate_task(base_rdd, '62', 4)

    union_rdd_9 = spark.sparkContext.union([task_rdd_4, task_rdd_5, task_rdd_8])
    task_rdd_9 = simulate_task(union_rdd_9, '9', 6)

    union_rdd_13 = spark.sparkContext.union([task_rdd_4, task_rdd_6, task_rdd_12])
    task_rdd_13 = simulate_task(union_rdd_13, '13', 198)

    union_rdd_21 = spark.sparkContext.union([task_rdd_16])
    task_rdd_21 = simulate_task(union_rdd_21, '21', 2457)

    union_rdd_17 = spark.sparkContext.union([task_rdd_5, task_rdd_6])
    task_rdd_17 = simulate_task(union_rdd_17, '17', 19)

    union_rdd_47 = spark.sparkContext.union([task_rdd_48])
    task_rdd_47 = simulate_task(union_rdd_47, '47', 16)

    union_rdd_67 = spark.sparkContext.union([task_rdd_34])
    task_rdd_67 = simulate_task(union_rdd_67, '67', 26)

    union_rdd_41 = spark.sparkContext.union([task_rdd_2, task_rdd_27, task_rdd_40])
    task_rdd_41 = simulate_task(union_rdd_41, '41', 26)

    union_rdd_63 = spark.sparkContext.union([task_rdd_62])
    task_rdd_63 = simulate_task(union_rdd_63, '63', 2)

    union_rdd_37 = spark.sparkContext.union([task_rdd_36])
    task_rdd_37 = simulate_task(union_rdd_37, '37', 6)

    union_rdd_30 = spark.sparkContext.union([task_rdd_3])
    task_rdd_30 = simulate_task(union_rdd_30, '30', 42)

    union_rdd_50 = spark.sparkContext.union([task_rdd_49])
    task_rdd_50 = simulate_task(union_rdd_50, '50', 244)

    union_rdd_57 = spark.sparkContext.union([task_rdd_42])
    task_rdd_57 = simulate_task(union_rdd_57, '57', 628)

    union_rdd_65 = spark.sparkContext.union([task_rdd_3, task_rdd_4, task_rdd_5, task_rdd_6, task_rdd_7, task_rdd_8, task_rdd_9, task_rdd_64])
    task_rdd_65 = simulate_task(union_rdd_65, '65', 34)

    union_rdd_14 = spark.sparkContext.union([task_rdd_11, task_rdd_13])
    task_rdd_14 = simulate_task(union_rdd_14, '14', 3)

    union_rdd_55 = spark.sparkContext.union([task_rdd_50, task_rdd_54])
    task_rdd_55 = simulate_task(union_rdd_55, '55', 8)

    union_rdd_45 = spark.sparkContext.union([task_rdd_33, task_rdd_36, task_rdd_37, task_rdd_38])
    task_rdd_45 = simulate_task(union_rdd_45, '45', 29)

    union_rdd_31 = spark.sparkContext.union([task_rdd_30])
    task_rdd_31 = simulate_task(union_rdd_31, '31', 25)

    union_rdd_58 = spark.sparkContext.union([task_rdd_57])
    task_rdd_58 = simulate_task(union_rdd_58, '58', 16)

    union_rdd_66 = spark.sparkContext.union([task_rdd_36, task_rdd_38, task_rdd_40, task_rdd_42, task_rdd_63, task_rdd_65])
    task_rdd_66 = simulate_task(union_rdd_66, '66', 5)

    union_rdd_15 = spark.sparkContext.union([task_rdd_2, task_rdd_4, task_rdd_6, task_rdd_12, task_rdd_14])
    task_rdd_15 = simulate_task(union_rdd_15, '15', 19)

    union_rdd_46 = spark.sparkContext.union([task_rdd_45])
    task_rdd_46 = simulate_task(union_rdd_46, '46', 14)

    union_rdd_32 = spark.sparkContext.union([task_rdd_26, task_rdd_31])
    task_rdd_32 = simulate_task(union_rdd_32, '32', 570)

    union_rdd_59 = spark.sparkContext.union([task_rdd_58, task_rdd_60])
    task_rdd_59 = simulate_task(union_rdd_59, '59', 24)

    union_rdd_43 = spark.sparkContext.union([task_rdd_66])
    task_rdd_43 = simulate_task(union_rdd_43, '43', 5)

    union_rdd_10 = spark.sparkContext.union([task_rdd_12, task_rdd_15])
    task_rdd_10 = simulate_task(union_rdd_10, '10', 4)

    union_rdd_52 = spark.sparkContext.union([task_rdd_59])
    task_rdd_52 = simulate_task(union_rdd_52, '52', 191)

    union_rdd_35 = spark.sparkContext.union([task_rdd_34, task_rdd_43])
    task_rdd_35 = simulate_task(union_rdd_35, '35', 317)

    union_rdd_56 = spark.sparkContext.union([task_rdd_51, task_rdd_52, task_rdd_53, task_rdd_54, task_rdd_55])
    task_rdd_56 = simulate_task(union_rdd_56, '56', 28)

    union_rdd_19 = spark.sparkContext.union([task_rdd_3, task_rdd_18, task_rdd_35])
    task_rdd_19 = simulate_task(union_rdd_19, '19', 76)

    union_rdd_22 = spark.sparkContext.union([task_rdd_56])
    task_rdd_22 = simulate_task(union_rdd_22, '22', 3)

    union_rdd_20 = spark.sparkContext.union([task_rdd_1, task_rdd_5, task_rdd_12, task_rdd_19])
    task_rdd_20 = simulate_task(union_rdd_20, '20', 22)

    union_rdd_23 = spark.sparkContext.union([task_rdd_22])
    task_rdd_23 = simulate_task(union_rdd_23, '23', 284)

    combined_rdd = spark.sparkContext.union([task_rdd_20])
    task_rdd = simulate_task(combined_rdd, 'M12_20', 16)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
