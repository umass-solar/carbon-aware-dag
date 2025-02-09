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

    task_rdd_1 = simulate_task(base_rdd, '1', 3)
    task_rdd_9 = simulate_task(base_rdd, '9', 161)
    task_rdd_43 = simulate_task(base_rdd, '43', 4)
    task_rdd_32 = simulate_task(base_rdd, '32', 2)
    task_rdd_17 = simulate_task(base_rdd, '17', 379)
    task_rdd_13 = simulate_task(base_rdd, '13', 6)
    task_rdd_3 = simulate_task(base_rdd, '3', 23)
    task_rdd_8 = simulate_task(base_rdd, '8', 3)
    task_rdd_2 = simulate_task(base_rdd, '2', 72)
    task_rdd_60 = simulate_task(base_rdd, '60', 5)
    task_rdd_15 = simulate_task(base_rdd, '15', 36)
    task_rdd_76 = simulate_task(base_rdd, '76', 1)
    task_rdd_73 = simulate_task(base_rdd, '73', 1)
    task_rdd_69 = simulate_task(base_rdd, '69', 15)
    task_rdd_49 = simulate_task(base_rdd, '49', 11)
    task_rdd_36 = simulate_task(base_rdd, '36', 92)
    task_rdd_34 = simulate_task(base_rdd, '34', 23)
    task_rdd_31 = simulate_task(base_rdd, '31', 4)
    task_rdd_42 = simulate_task(base_rdd, '42', 25)
    task_rdd_75 = simulate_task(base_rdd, '75', 28)

    union_rdd_5 = spark.sparkContext.union([task_rdd_1])
    task_rdd_5 = simulate_task(union_rdd_5, '5', 1)

    union_rdd_10 = spark.sparkContext.union([task_rdd_3, task_rdd_9])
    task_rdd_10 = simulate_task(union_rdd_10, '10', 123)

    union_rdd_24 = spark.sparkContext.union([task_rdd_43])
    task_rdd_24 = simulate_task(union_rdd_24, '24', 26)

    union_rdd_28 = spark.sparkContext.union([task_rdd_42])
    task_rdd_28 = simulate_task(union_rdd_28, '28', 5)

    union_rdd_33 = spark.sparkContext.union([task_rdd_32])
    task_rdd_33 = simulate_task(union_rdd_33, '33', 334)

    union_rdd_14 = spark.sparkContext.union([task_rdd_3, task_rdd_13])
    task_rdd_14 = simulate_task(union_rdd_14, '14', 338)

    union_rdd_4 = spark.sparkContext.union([task_rdd_75])
    task_rdd_4 = simulate_task(union_rdd_4, '4', 12)

    union_rdd_21 = spark.sparkContext.union([task_rdd_60])
    task_rdd_21 = simulate_task(union_rdd_21, '21', 9)

    union_rdd_70 = spark.sparkContext.union([task_rdd_69])
    task_rdd_70 = simulate_task(union_rdd_70, '70', 7)

    union_rdd_20 = spark.sparkContext.union([task_rdd_36])
    task_rdd_20 = simulate_task(union_rdd_20, '20', 197)

    union_rdd_35 = spark.sparkContext.union([task_rdd_31, task_rdd_34])
    task_rdd_35 = simulate_task(union_rdd_35, '35', 2)

    union_rdd_6 = spark.sparkContext.union([task_rdd_1, task_rdd_2, task_rdd_4, task_rdd_5])
    task_rdd_6 = simulate_task(union_rdd_6, '6', 12)

    union_rdd_11 = spark.sparkContext.union([task_rdd_2, task_rdd_9, task_rdd_10])
    task_rdd_11 = simulate_task(union_rdd_11, '11', 170)

    union_rdd_48 = spark.sparkContext.union([task_rdd_24])
    task_rdd_48 = simulate_task(union_rdd_48, '48', 8)

    union_rdd_22 = spark.sparkContext.union([task_rdd_21])
    task_rdd_22 = simulate_task(union_rdd_22, '22', 95)

    union_rdd_44 = spark.sparkContext.union([task_rdd_20])
    task_rdd_44 = simulate_task(union_rdd_44, '44', 401)

    union_rdd_12 = spark.sparkContext.union([task_rdd_2, task_rdd_4, task_rdd_8, task_rdd_9, task_rdd_11])
    task_rdd_12 = simulate_task(union_rdd_12, '12', 23)

    union_rdd_29 = spark.sparkContext.union([task_rdd_28, task_rdd_48])
    task_rdd_29 = simulate_task(union_rdd_29, '29', 37)

    union_rdd_23 = spark.sparkContext.union([task_rdd_22])
    task_rdd_23 = simulate_task(union_rdd_23, '23', 260)

    union_rdd_45 = spark.sparkContext.union([task_rdd_36, task_rdd_44, task_rdd_49, task_rdd_70, task_rdd_73, task_rdd_76])
    task_rdd_45 = simulate_task(union_rdd_45, '45', 31)

    union_rdd_25 = spark.sparkContext.union([task_rdd_15, task_rdd_28])
    task_rdd_25 = simulate_task(union_rdd_25, '25', 5)

    union_rdd_18 = spark.sparkContext.union([task_rdd_13, task_rdd_17])
    task_rdd_18 = simulate_task(union_rdd_18, '18', 30)

    union_rdd_16 = spark.sparkContext.union([task_rdd_13, task_rdd_14])
    task_rdd_16 = simulate_task(union_rdd_16, '16', 58)

    union_rdd_19 = spark.sparkContext.union([task_rdd_18])
    task_rdd_19 = simulate_task(union_rdd_19, '19', 3)

    union_rdd_26 = spark.sparkContext.union([task_rdd_17, task_rdd_19])
    task_rdd_26 = simulate_task(union_rdd_26, '26', 73)

    combined_rdd = spark.sparkContext.union([task_rdd_1, task_rdd_2, task_rdd_3, task_rdd_4, task_rdd_6])
    task_rdd = simulate_task(combined_rdd, 'J7_1_2_3_4_6', 3)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
