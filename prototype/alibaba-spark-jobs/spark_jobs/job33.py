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

    task_rdd_11 = simulate_task(base_rdd, '11', 10)
    task_rdd_132 = simulate_task(base_rdd, '132', 4)
    task_rdd_55 = simulate_task(base_rdd, '55', 5)
    task_rdd_2 = simulate_task(base_rdd, '2', 6)
    task_rdd_44 = simulate_task(base_rdd, '44', 13)
    task_rdd_1 = simulate_task(base_rdd, '1', 26)
    task_rdd_42 = simulate_task(base_rdd, '42', 24)
    task_rdd_4 = simulate_task(base_rdd, '4', 27)
    task_rdd_30 = simulate_task(base_rdd, '30', 212)
    task_rdd_32 = simulate_task(base_rdd, '32', 5)
    task_rdd_3 = simulate_task(base_rdd, '3', 4)
    task_rdd_14 = simulate_task(base_rdd, '14', 1)
    task_rdd_39 = simulate_task(base_rdd, '39', 17)
    task_rdd_5 = simulate_task(base_rdd, '5', 116)
    task_rdd_6 = simulate_task(base_rdd, '6', 2)
    task_rdd_15 = simulate_task(base_rdd, '15', 8)
    task_rdd_34 = simulate_task(base_rdd, '34', 2)
    task_rdd_27 = simulate_task(base_rdd, '27', 8)
    task_rdd_120 = simulate_task(base_rdd, '120', 4)
    task_rdd_21 = simulate_task(base_rdd, '21', 5)
    task_rdd_67 = simulate_task(base_rdd, '67', 119)
    task_rdd_50 = simulate_task(base_rdd, '50', 181)
    task_rdd_28 = simulate_task(base_rdd, '28', 14)
    task_rdd_16 = simulate_task(base_rdd, '16', 115)

    union_rdd_73 = spark.sparkContext.union([task_rdd_132])
    task_rdd_73 = simulate_task(union_rdd_73, '73', 4)

    union_rdd_63 = spark.sparkContext.union([task_rdd_55])
    task_rdd_63 = simulate_task(union_rdd_63, '63', 155)

    union_rdd_61 = spark.sparkContext.union([task_rdd_2])
    task_rdd_61 = simulate_task(union_rdd_61, '61', 117)

    union_rdd_7 = spark.sparkContext.union([task_rdd_16])
    task_rdd_7 = simulate_task(union_rdd_7, '7', 1)

    union_rdd_12 = spark.sparkContext.union([task_rdd_1, task_rdd_11])
    task_rdd_12 = simulate_task(union_rdd_12, '12', 12)

    union_rdd_43 = spark.sparkContext.union([task_rdd_4, task_rdd_42])
    task_rdd_43 = simulate_task(union_rdd_43, '43', 20)

    union_rdd_33 = spark.sparkContext.union([task_rdd_3, task_rdd_32])
    task_rdd_33 = simulate_task(union_rdd_33, '33', 10)

    union_rdd_40 = spark.sparkContext.union([task_rdd_5, task_rdd_39])
    task_rdd_40 = simulate_task(union_rdd_40, '40', 237)

    union_rdd_25 = spark.sparkContext.union([task_rdd_34])
    task_rdd_25 = simulate_task(union_rdd_25, '25', 8)

    union_rdd_68 = spark.sparkContext.union([task_rdd_67])
    task_rdd_68 = simulate_task(union_rdd_68, '68', 18)

    union_rdd_62 = spark.sparkContext.union([task_rdd_61])
    task_rdd_62 = simulate_task(union_rdd_62, '62', 43)

    union_rdd_8 = spark.sparkContext.union([task_rdd_3, task_rdd_4, task_rdd_7])
    task_rdd_8 = simulate_task(union_rdd_8, '8', 4)

    union_rdd_13 = spark.sparkContext.union([task_rdd_11, task_rdd_12])
    task_rdd_13 = simulate_task(union_rdd_13, '13', 15)

    union_rdd_29 = spark.sparkContext.union([task_rdd_33])
    task_rdd_29 = simulate_task(union_rdd_29, '29', 2)

    union_rdd_26 = spark.sparkContext.union([task_rdd_25])
    task_rdd_26 = simulate_task(union_rdd_26, '26', 69)

    union_rdd_17 = spark.sparkContext.union([task_rdd_39, task_rdd_40])
    task_rdd_17 = simulate_task(union_rdd_17, '17', 12)

    union_rdd_51 = spark.sparkContext.union([task_rdd_68])
    task_rdd_51 = simulate_task(union_rdd_51, '51', 4)

    union_rdd_64 = spark.sparkContext.union([task_rdd_62, task_rdd_63, task_rdd_73])
    task_rdd_64 = simulate_task(union_rdd_64, '64', 12)

    union_rdd_31 = spark.sparkContext.union([task_rdd_26, task_rdd_30])
    task_rdd_31 = simulate_task(union_rdd_31, '31', 11)

    union_rdd_18 = spark.sparkContext.union([task_rdd_17])
    task_rdd_18 = simulate_task(union_rdd_18, '18', 9)

    union_rdd_45 = spark.sparkContext.union([task_rdd_51])
    task_rdd_45 = simulate_task(union_rdd_45, '45', 2)

    union_rdd_65 = spark.sparkContext.union([task_rdd_21, task_rdd_45, task_rdd_64])
    task_rdd_65 = simulate_task(union_rdd_65, '65', 401)

    union_rdd_9 = spark.sparkContext.union([task_rdd_31, task_rdd_43])
    task_rdd_9 = simulate_task(union_rdd_9, '9', 66)

    union_rdd_19 = spark.sparkContext.union([task_rdd_5, task_rdd_6, task_rdd_18])
    task_rdd_19 = simulate_task(union_rdd_19, '19', 11)

    union_rdd_66 = spark.sparkContext.union([task_rdd_50, task_rdd_51, task_rdd_65])
    task_rdd_66 = simulate_task(union_rdd_66, '66', 21)

    union_rdd_20 = spark.sparkContext.union([task_rdd_19])
    task_rdd_20 = simulate_task(union_rdd_20, '20', 1)

    union_rdd_36 = spark.sparkContext.union([task_rdd_66])
    task_rdd_36 = simulate_task(union_rdd_36, '36', 2)

    union_rdd_22 = spark.sparkContext.union([task_rdd_21, task_rdd_28, task_rdd_30, task_rdd_32, task_rdd_34, task_rdd_36])
    task_rdd_22 = simulate_task(union_rdd_22, '22', 112)

    union_rdd_23 = spark.sparkContext.union([task_rdd_4, task_rdd_5, task_rdd_7, task_rdd_22])
    task_rdd_23 = simulate_task(union_rdd_23, '23', 109)

    union_rdd_24 = spark.sparkContext.union([task_rdd_2, task_rdd_4, task_rdd_6, task_rdd_7, task_rdd_23])
    task_rdd_24 = simulate_task(union_rdd_24, '24', 163)

    union_rdd_10 = spark.sparkContext.union([task_rdd_24])
    task_rdd_10 = simulate_task(union_rdd_10, '10', 27)

    combined_rdd = spark.sparkContext.union([task_rdd_5, task_rdd_6, task_rdd_10])
    task_rdd = simulate_task(combined_rdd, 'J11_5_6_10', 1)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
