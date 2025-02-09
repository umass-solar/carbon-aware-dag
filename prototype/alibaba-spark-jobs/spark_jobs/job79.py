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

    task_rdd_13 = simulate_task(base_rdd, '13', 12)
    task_rdd_10 = simulate_task(base_rdd, '10', 14)
    task_rdd_9 = simulate_task(base_rdd, '9', 3)
    task_rdd_8 = simulate_task(base_rdd, '8', 17)
    task_rdd_7 = simulate_task(base_rdd, '7', 28)
    task_rdd_6 = simulate_task(base_rdd, '6', 2)
    task_rdd_3 = simulate_task(base_rdd, '3', 2)
    task_rdd_130 = simulate_task(base_rdd, '130', 4)
    task_rdd_1 = simulate_task(base_rdd, '1', 15)
    task_rdd_5 = simulate_task(base_rdd, '5', 6)
    task_rdd_4 = simulate_task(base_rdd, '4', 5)
    task_rdd_25 = simulate_task(base_rdd, '25', 12)
    task_rdd_46 = simulate_task(base_rdd, '46', 12)
    task_rdd_37 = simulate_task(base_rdd, '37', 11)
    task_rdd_29 = simulate_task(base_rdd, '29', 11)
    task_rdd_2 = simulate_task(base_rdd, '2', 60)
    task_rdd_19 = simulate_task(base_rdd, '19', 164)
    task_rdd_17 = simulate_task(base_rdd, '17', 14)
    task_rdd_28 = simulate_task(base_rdd, '28', 8)
    task_rdd_26 = simulate_task(base_rdd, '26', 181)
    task_rdd_22 = simulate_task(base_rdd, '22', 2)
    task_rdd_21 = simulate_task(base_rdd, '21', 27)
    task_rdd_48 = simulate_task(base_rdd, '48', 2)
    task_rdd_23 = simulate_task(base_rdd, '23', 301)

    union_rdd_11 = spark.sparkContext.union([task_rdd_6, task_rdd_10])
    task_rdd_11 = simulate_task(union_rdd_11, '11', 7)

    union_rdd_14 = spark.sparkContext.union([task_rdd_3, task_rdd_13])
    task_rdd_14 = simulate_task(union_rdd_14, '14', 52)

    union_rdd_131 = spark.sparkContext.union([task_rdd_130])
    task_rdd_131 = simulate_task(union_rdd_131, '131', 3)

    union_rdd_69 = spark.sparkContext.union([task_rdd_1])
    task_rdd_69 = simulate_task(union_rdd_69, '69', 93)

    union_rdd_34 = spark.sparkContext.union([task_rdd_21, task_rdd_22])
    task_rdd_34 = simulate_task(union_rdd_34, '34', 167)

    union_rdd_49 = spark.sparkContext.union([task_rdd_48])
    task_rdd_49 = simulate_task(union_rdd_49, '49', 31)

    union_rdd_47 = spark.sparkContext.union([task_rdd_46])
    task_rdd_47 = simulate_task(union_rdd_47, '47', 88)

    union_rdd_30 = spark.sparkContext.union([task_rdd_28])
    task_rdd_30 = simulate_task(union_rdd_30, '30', 258)

    union_rdd_18 = spark.sparkContext.union([task_rdd_17])
    task_rdd_18 = simulate_task(union_rdd_18, '18', 148)

    union_rdd_24 = spark.sparkContext.union([task_rdd_23])
    task_rdd_24 = simulate_task(union_rdd_24, '24', 48)

    union_rdd_12 = spark.sparkContext.union([task_rdd_11])
    task_rdd_12 = simulate_task(union_rdd_12, '12', 47)

    union_rdd_15 = spark.sparkContext.union([task_rdd_1, task_rdd_2, task_rdd_4, task_rdd_5, task_rdd_13, task_rdd_14])
    task_rdd_15 = simulate_task(union_rdd_15, '15', 62)

    union_rdd_132 = spark.sparkContext.union([task_rdd_131])
    task_rdd_132 = simulate_task(union_rdd_132, '132', 1)

    union_rdd_53 = spark.sparkContext.union([task_rdd_69])
    task_rdd_53 = simulate_task(union_rdd_53, '53', 2)

    union_rdd_35 = spark.sparkContext.union([task_rdd_34])
    task_rdd_35 = simulate_task(union_rdd_35, '35', 33)

    union_rdd_45 = spark.sparkContext.union([task_rdd_47, task_rdd_49])
    task_rdd_45 = simulate_task(union_rdd_45, '45', 7)

    union_rdd_38 = spark.sparkContext.union([task_rdd_24])
    task_rdd_38 = simulate_task(union_rdd_38, '38', 47)

    union_rdd_16 = spark.sparkContext.union([task_rdd_1, task_rdd_3, task_rdd_4, task_rdd_15])
    task_rdd_16 = simulate_task(union_rdd_16, '16', 233)

    union_rdd_120 = spark.sparkContext.union([task_rdd_132])
    task_rdd_120 = simulate_task(union_rdd_120, '120', 4)

    union_rdd_52 = spark.sparkContext.union([task_rdd_53])
    task_rdd_52 = simulate_task(union_rdd_52, '52', 75)

    union_rdd_36 = spark.sparkContext.union([task_rdd_17, task_rdd_22, task_rdd_34, task_rdd_35])
    task_rdd_36 = simulate_task(union_rdd_36, '36', 151)

    union_rdd_39 = spark.sparkContext.union([task_rdd_2, task_rdd_30, task_rdd_38])
    task_rdd_39 = simulate_task(union_rdd_39, '39', 136)

    union_rdd_42 = spark.sparkContext.union([task_rdd_16])
    task_rdd_42 = simulate_task(union_rdd_42, '42', 70)

    union_rdd_95 = spark.sparkContext.union([task_rdd_120])
    task_rdd_95 = simulate_task(union_rdd_95, '95', 4)

    union_rdd_20 = spark.sparkContext.union([task_rdd_19, task_rdd_36])
    task_rdd_20 = simulate_task(union_rdd_20, '20', 472)

    union_rdd_40 = spark.sparkContext.union([task_rdd_39, task_rdd_45])
    task_rdd_40 = simulate_task(union_rdd_40, '40', 27)

    union_rdd_55 = spark.sparkContext.union([task_rdd_42])
    task_rdd_55 = simulate_task(union_rdd_55, '55', 11)

    union_rdd_100 = spark.sparkContext.union([task_rdd_95])
    task_rdd_100 = simulate_task(union_rdd_100, '100', 2)

    union_rdd_31 = spark.sparkContext.union([task_rdd_40])
    task_rdd_31 = simulate_task(union_rdd_31, '31', 35)

    union_rdd_50 = spark.sparkContext.union([task_rdd_25, task_rdd_38, task_rdd_46, task_rdd_49, task_rdd_55])
    task_rdd_50 = simulate_task(union_rdd_50, '50', 398)

    union_rdd_27 = spark.sparkContext.union([task_rdd_100])
    task_rdd_27 = simulate_task(union_rdd_27, '27', 15)

    union_rdd_32 = spark.sparkContext.union([task_rdd_31])
    task_rdd_32 = simulate_task(union_rdd_32, '32', 378)

    union_rdd_51 = spark.sparkContext.union([task_rdd_50])
    task_rdd_51 = simulate_task(union_rdd_51, '51', 14)

    union_rdd_33 = spark.sparkContext.union([task_rdd_26, task_rdd_28, task_rdd_30, task_rdd_32])
    task_rdd_33 = simulate_task(union_rdd_33, '33', 3)

    combined_rdd = spark.sparkContext.union([task_rdd_1, task_rdd_2, task_rdd_5, task_rdd_8, task_rdd_9, task_rdd_10, task_rdd_12])
    task_rdd = simulate_task(combined_rdd, 'J13_1_2_5_8_9_10_12', 17)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
