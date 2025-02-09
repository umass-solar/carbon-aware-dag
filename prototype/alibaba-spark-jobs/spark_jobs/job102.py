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

    task_rdd_3 = simulate_task(base_rdd, '3', 1)
    task_rdd_5 = simulate_task(base_rdd, '5', 259)
    task_rdd_8 = simulate_task(base_rdd, '8', 156)
    task_rdd_4 = simulate_task(base_rdd, '4', 4)
    task_rdd_2 = simulate_task(base_rdd, '2', 114)
    task_rdd_17 = simulate_task(base_rdd, '17', 7)
    task_rdd_15 = simulate_task(base_rdd, '15', 26)
    task_rdd_33 = simulate_task(base_rdd, '33', 3)
    task_rdd_14 = simulate_task(base_rdd, '14', 56)
    task_rdd_25 = simulate_task(base_rdd, '25', 28)
    task_rdd_49 = simulate_task(base_rdd, '49', 5)
    task_rdd_22 = simulate_task(base_rdd, '22', 84)
    task_rdd_19 = simulate_task(base_rdd, '19', 2)
    task_rdd_27 = simulate_task(base_rdd, '27', 13)
    task_rdd_54 = simulate_task(base_rdd, '54', 50)
    task_rdd_34 = simulate_task(base_rdd, '34', 151)
    task_rdd_32 = simulate_task(base_rdd, '32', 17)
    task_rdd_23 = simulate_task(base_rdd, '23', 138)
    task_rdd_35 = simulate_task(base_rdd, '35', 4)
    task_rdd_79 = simulate_task(base_rdd, '79', 16)
    task_rdd_128 = simulate_task(base_rdd, '128', 8)

    union_rdd_18 = spark.sparkContext.union([task_rdd_2])
    task_rdd_18 = simulate_task(union_rdd_18, '18', 1)

    union_rdd_24 = spark.sparkContext.union([task_rdd_33])
    task_rdd_24 = simulate_task(union_rdd_24, '24', 1)

    union_rdd_26 = spark.sparkContext.union([task_rdd_25])
    task_rdd_26 = simulate_task(union_rdd_26, '26', 162)

    union_rdd_59 = spark.sparkContext.union([task_rdd_49])
    task_rdd_59 = simulate_task(union_rdd_59, '59', 3)

    union_rdd_21 = spark.sparkContext.union([task_rdd_35])
    task_rdd_21 = simulate_task(union_rdd_21, '21', 14)

    union_rdd_20 = spark.sparkContext.union([task_rdd_19])
    task_rdd_20 = simulate_task(union_rdd_20, '20', 211)

    union_rdd_50 = spark.sparkContext.union([task_rdd_32, task_rdd_34])
    task_rdd_50 = simulate_task(union_rdd_50, '50', 623)

    union_rdd_129 = spark.sparkContext.union([task_rdd_128])
    task_rdd_129 = simulate_task(union_rdd_129, '129', 5)

    union_rdd_1 = spark.sparkContext.union([task_rdd_18])
    task_rdd_1 = simulate_task(union_rdd_1, '1', 151)

    union_rdd_31 = spark.sparkContext.union([task_rdd_26])
    task_rdd_31 = simulate_task(union_rdd_31, '31', 425)

    union_rdd_30 = spark.sparkContext.union([task_rdd_59])
    task_rdd_30 = simulate_task(union_rdd_30, '30', 17)

    union_rdd_29 = spark.sparkContext.union([task_rdd_19, task_rdd_20, task_rdd_21, task_rdd_22])
    task_rdd_29 = simulate_task(union_rdd_29, '29', 8)

    union_rdd_51 = spark.sparkContext.union([task_rdd_34, task_rdd_50])
    task_rdd_51 = simulate_task(union_rdd_51, '51', 2)

    union_rdd_130 = spark.sparkContext.union([task_rdd_129])
    task_rdd_130 = simulate_task(union_rdd_130, '130', 1)

    union_rdd_53 = spark.sparkContext.union([task_rdd_15, task_rdd_17, task_rdd_19, task_rdd_21, task_rdd_23, task_rdd_25, task_rdd_27, task_rdd_29, task_rdd_30, task_rdd_31, task_rdd_3])
    task_rdd_53 = simulate_task(union_rdd_53, '53', 234)

    union_rdd_16 = spark.sparkContext.union([task_rdd_31])
    task_rdd_16 = simulate_task(union_rdd_16, '16', 15)

    union_rdd_55 = spark.sparkContext.union([task_rdd_51, task_rdd_54])
    task_rdd_55 = simulate_task(union_rdd_55, '55', 45)

    union_rdd_131 = spark.sparkContext.union([task_rdd_130])
    task_rdd_131 = simulate_task(union_rdd_131, '131', 3)

    union_rdd_132 = spark.sparkContext.union([task_rdd_131])
    task_rdd_132 = simulate_task(union_rdd_132, '132', 1)

    union_rdd_44 = spark.sparkContext.union([task_rdd_132])
    task_rdd_44 = simulate_task(union_rdd_44, '44', 4)

    union_rdd_57 = spark.sparkContext.union([task_rdd_44])
    task_rdd_57 = simulate_task(union_rdd_57, '57', 24)

    union_rdd_58 = spark.sparkContext.union([task_rdd_57, task_rdd_79])
    task_rdd_58 = simulate_task(union_rdd_58, '58', 4)

    union_rdd_13 = spark.sparkContext.union([])
    task_rdd_13 = simulate_task(union_rdd_13, '13', 217)

    union_rdd_28 = spark.sparkContext.union([task_rdd_13])
    task_rdd_28 = simulate_task(union_rdd_28, '28', 163)

    union_rdd_9 = spark.sparkContext.union([task_rdd_28])
    task_rdd_9 = simulate_task(union_rdd_9, '9', 384)

    union_rdd_10 = spark.sparkContext.union([task_rdd_4, task_rdd_5, task_rdd_9])
    task_rdd_10 = simulate_task(union_rdd_10, '10', 89)

    union_rdd_11 = spark.sparkContext.union([task_rdd_2, task_rdd_3, task_rdd_10])
    task_rdd_11 = simulate_task(union_rdd_11, '11', 4)

    union_rdd_12 = spark.sparkContext.union([task_rdd_11])
    task_rdd_12 = simulate_task(union_rdd_12, '12', 162)

    union_rdd_6 = spark.sparkContext.union([task_rdd_4, task_rdd_5, task_rdd_12])
    task_rdd_6 = simulate_task(union_rdd_6, '6', 279)

    combined_rdd = spark.sparkContext.union([task_rdd_2, task_rdd_3, task_rdd_4, task_rdd_6])
    task_rdd = simulate_task(combined_rdd, 'J7_2_3_4_6', 4)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
