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

    task_rdd_15 = simulate_task(base_rdd, '15', 7)
    task_rdd_14 = simulate_task(base_rdd, '14', 93)
    task_rdd_13 = simulate_task(base_rdd, '13', 6)
    task_rdd_12 = simulate_task(base_rdd, '12', 2)
    task_rdd_5 = simulate_task(base_rdd, '5', 2)
    task_rdd_11 = simulate_task(base_rdd, '11', 2)
    task_rdd_10 = simulate_task(base_rdd, '10', 6)
    task_rdd_45 = simulate_task(base_rdd, '45', 3)
    task_rdd_31 = simulate_task(base_rdd, '31', 13)
    task_rdd_2 = simulate_task(base_rdd, '2', 3)
    task_rdd_20 = simulate_task(base_rdd, '20', 98)
    task_rdd_19 = simulate_task(base_rdd, '19', 7)
    task_rdd_23 = simulate_task(base_rdd, '23', 478)
    task_rdd_18 = simulate_task(base_rdd, '18', 13)
    task_rdd_9 = simulate_task(base_rdd, '9', 18)
    task_rdd_8 = simulate_task(base_rdd, '8', 9)
    task_rdd_3 = simulate_task(base_rdd, '3', 38)
    task_rdd_7 = simulate_task(base_rdd, '7', 2)
    task_rdd_54 = simulate_task(base_rdd, '54', 11)
    task_rdd_40 = simulate_task(base_rdd, '40', 22)
    task_rdd_100 = simulate_task(base_rdd, '100', 36)
    task_rdd_1 = simulate_task(base_rdd, '1', 9)
    task_rdd_25 = simulate_task(base_rdd, '25', 2)
    task_rdd_60 = simulate_task(base_rdd, '60', 4)
    task_rdd_37 = simulate_task(base_rdd, '37', 45)
    task_rdd_26 = simulate_task(base_rdd, '26', 11)
    task_rdd_67 = simulate_task(base_rdd, '67', 28)
    task_rdd_38 = simulate_task(base_rdd, '38', 16)
    task_rdd_6 = simulate_task(base_rdd, '6', 143)
    task_rdd_53 = simulate_task(base_rdd, '53', 9)
    task_rdd_51 = simulate_task(base_rdd, '51', 24)
    task_rdd_87 = simulate_task(base_rdd, '87', 5)

    union_rdd_16 = spark.sparkContext.union([task_rdd_3, task_rdd_11, task_rdd_15])
    task_rdd_16 = simulate_task(union_rdd_16, '16', 151)

    union_rdd_32 = spark.sparkContext.union([task_rdd_2, task_rdd_6, task_rdd_7, task_rdd_8, task_rdd_9, task_rdd_31])
    task_rdd_32 = simulate_task(union_rdd_32, '32', 74)

    union_rdd_21 = spark.sparkContext.union([task_rdd_18, task_rdd_19, task_rdd_20])
    task_rdd_21 = simulate_task(union_rdd_21, '21', 3)

    union_rdd_55 = spark.sparkContext.union([task_rdd_87])
    task_rdd_55 = simulate_task(union_rdd_55, '55', 469)

    union_rdd_43 = spark.sparkContext.union([task_rdd_100])
    task_rdd_43 = simulate_task(union_rdd_43, '43', 15)

    union_rdd_72 = spark.sparkContext.union([task_rdd_25])
    task_rdd_72 = simulate_task(union_rdd_72, '72', 10)

    union_rdd_48 = spark.sparkContext.union([task_rdd_38])
    task_rdd_48 = simulate_task(union_rdd_48, '48', 55)

    union_rdd_62 = spark.sparkContext.union([task_rdd_60])
    task_rdd_62 = simulate_task(union_rdd_62, '62', 4)

    union_rdd_27 = spark.sparkContext.union([task_rdd_37])
    task_rdd_27 = simulate_task(union_rdd_27, '27', 3)

    union_rdd_68 = spark.sparkContext.union([task_rdd_67])
    task_rdd_68 = simulate_task(union_rdd_68, '68', 7)

    union_rdd_57 = spark.sparkContext.union([task_rdd_53])
    task_rdd_57 = simulate_task(union_rdd_57, '57', 2)

    union_rdd_52 = spark.sparkContext.union([task_rdd_51])
    task_rdd_52 = simulate_task(union_rdd_52, '52', 219)

    union_rdd_17 = spark.sparkContext.union([task_rdd_1, task_rdd_2, task_rdd_3, task_rdd_5, task_rdd_6, task_rdd_7, task_rdd_8, task_rdd_9, task_rdd_10, task_rdd_11, task_rdd_12, task_rdd_13, task_rdd_14, task_rdd_15, task_rdd_16])
    task_rdd_17 = simulate_task(union_rdd_17, '17', 109)

    union_rdd_33 = spark.sparkContext.union([task_rdd_8, task_rdd_32])
    task_rdd_33 = simulate_task(union_rdd_33, '33', 6)

    union_rdd_22 = spark.sparkContext.union([task_rdd_1, task_rdd_3, task_rdd_7, task_rdd_21])
    task_rdd_22 = simulate_task(union_rdd_22, '22', 22)

    union_rdd_44 = spark.sparkContext.union([task_rdd_1, task_rdd_43])
    task_rdd_44 = simulate_task(union_rdd_44, '44', 1)

    union_rdd_28 = spark.sparkContext.union([task_rdd_26, task_rdd_27, task_rdd_48])
    task_rdd_28 = simulate_task(union_rdd_28, '28', 13)

    union_rdd_24 = spark.sparkContext.union([task_rdd_17])
    task_rdd_24 = simulate_task(union_rdd_24, '24', 110)

    union_rdd_34 = spark.sparkContext.union([task_rdd_8, task_rdd_20, task_rdd_33])
    task_rdd_34 = simulate_task(union_rdd_34, '34', 149)

    union_rdd_50 = spark.sparkContext.union([task_rdd_22])
    task_rdd_50 = simulate_task(union_rdd_50, '50', 217)

    union_rdd_75 = spark.sparkContext.union([task_rdd_44])
    task_rdd_75 = simulate_task(union_rdd_75, '75', 28)

    union_rdd_29 = spark.sparkContext.union([task_rdd_28])
    task_rdd_29 = simulate_task(union_rdd_29, '29', 50)

    union_rdd_39 = spark.sparkContext.union([])
    task_rdd_39 = simulate_task(union_rdd_39, '39', 32)

    union_rdd_36 = spark.sparkContext.union([task_rdd_45])
    task_rdd_36 = simulate_task(union_rdd_36, '36', 32)

    union_rdd_47 = spark.sparkContext.union([task_rdd_1])
    task_rdd_47 = simulate_task(union_rdd_47, '47', 14)

    union_rdd_70 = spark.sparkContext.union([task_rdd_40])
    task_rdd_70 = simulate_task(union_rdd_70, '70', 488)

    union_rdd_73 = spark.sparkContext.union([task_rdd_72])
    task_rdd_73 = simulate_task(union_rdd_73, '73', 16)

    union_rdd_58 = spark.sparkContext.union([task_rdd_47])
    task_rdd_58 = simulate_task(union_rdd_58, '58', 198)

    union_rdd_71 = spark.sparkContext.union([task_rdd_44, task_rdd_70])
    task_rdd_71 = simulate_task(union_rdd_71, '71', 72)

    union_rdd_59 = spark.sparkContext.union([task_rdd_53, task_rdd_54, task_rdd_55, task_rdd_57, task_rdd_58])
    task_rdd_59 = simulate_task(union_rdd_59, '59', 33)

    union_rdd_61 = spark.sparkContext.union([task_rdd_71])
    task_rdd_61 = simulate_task(union_rdd_61, '61', 243)

    union_rdd_49 = spark.sparkContext.union([task_rdd_48, task_rdd_61])
    task_rdd_49 = simulate_task(union_rdd_49, '49', 260)

    combined_rdd = spark.sparkContext.union([task_rdd_39])
    task_rdd = simulate_task(combined_rdd, 'R30_39', 22)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
