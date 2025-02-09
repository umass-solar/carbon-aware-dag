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

    task_rdd_11 = simulate_task(base_rdd, '11', 23)
    task_rdd_9 = simulate_task(base_rdd, '9', 19)
    task_rdd_8 = simulate_task(base_rdd, '8', 3)
    task_rdd_7 = simulate_task(base_rdd, '7', 3)
    task_rdd_88 = simulate_task(base_rdd, '88', 7)
    task_rdd_28 = simulate_task(base_rdd, '28', 9)
    task_rdd_21 = simulate_task(base_rdd, '21', 5)
    task_rdd_19 = simulate_task(base_rdd, '19', 6)
    task_rdd_18 = simulate_task(base_rdd, '18', 5)
    task_rdd_22 = simulate_task(base_rdd, '22', 1)
    task_rdd_5 = simulate_task(base_rdd, '5', 92)
    task_rdd_4 = simulate_task(base_rdd, '4', 7)
    task_rdd_16 = simulate_task(base_rdd, '16', 21)
    task_rdd_6 = simulate_task(base_rdd, '6', 57)
    task_rdd_3 = simulate_task(base_rdd, '3', 1)
    task_rdd_40 = simulate_task(base_rdd, '40', 13)
    task_rdd_38 = simulate_task(base_rdd, '38', 59)
    task_rdd_36 = simulate_task(base_rdd, '36', 17)
    task_rdd_57 = simulate_task(base_rdd, '57', 114)
    task_rdd_31 = simulate_task(base_rdd, '31', 9)
    task_rdd_34 = simulate_task(base_rdd, '34', 68)
    task_rdd_47 = simulate_task(base_rdd, '47', 13)
    task_rdd_24 = simulate_task(base_rdd, '24', 10)
    task_rdd_1 = simulate_task(base_rdd, '1', 34)
    task_rdd_49 = simulate_task(base_rdd, '49', 10)
    task_rdd_44 = simulate_task(base_rdd, '44', 127)

    union_rdd_10 = spark.sparkContext.union([task_rdd_4, task_rdd_7, task_rdd_8, task_rdd_9])
    task_rdd_10 = simulate_task(union_rdd_10, '10', 52)

    union_rdd_62 = spark.sparkContext.union([task_rdd_88])
    task_rdd_62 = simulate_task(union_rdd_62, '62', 3)

    union_rdd_29 = spark.sparkContext.union([task_rdd_4, task_rdd_16, task_rdd_19, task_rdd_21, task_rdd_28])
    task_rdd_29 = simulate_task(union_rdd_29, '29', 15)

    union_rdd_20 = spark.sparkContext.union([task_rdd_19])
    task_rdd_20 = simulate_task(union_rdd_20, '20', 15)

    union_rdd_23 = spark.sparkContext.union([task_rdd_5])
    task_rdd_23 = simulate_task(union_rdd_23, '23', 173)

    union_rdd_14 = spark.sparkContext.union([task_rdd_1])
    task_rdd_14 = simulate_task(union_rdd_14, '14', 17)

    union_rdd_39 = spark.sparkContext.union([task_rdd_38])
    task_rdd_39 = simulate_task(union_rdd_39, '39', 693)

    union_rdd_46 = spark.sparkContext.union([task_rdd_34])
    task_rdd_46 = simulate_task(union_rdd_46, '46', 2)

    union_rdd_48 = spark.sparkContext.union([task_rdd_44])
    task_rdd_48 = simulate_task(union_rdd_48, '48', 45)

    union_rdd_12 = spark.sparkContext.union([task_rdd_9, task_rdd_10, task_rdd_11])
    task_rdd_12 = simulate_task(union_rdd_12, '12', 332)

    union_rdd_30 = spark.sparkContext.union([task_rdd_4, task_rdd_5, task_rdd_6, task_rdd_14, task_rdd_16, task_rdd_18, task_rdd_19, task_rdd_21, task_rdd_22, task_rdd_24, task_rdd_29])
    task_rdd_30 = simulate_task(union_rdd_30, '30', 6)

    union_rdd_15 = spark.sparkContext.union([task_rdd_14, task_rdd_22])
    task_rdd_15 = simulate_task(union_rdd_15, '15', 58)

    union_rdd_2 = spark.sparkContext.union([task_rdd_48])
    task_rdd_2 = simulate_task(union_rdd_2, '2', 107)

    union_rdd_41 = spark.sparkContext.union([task_rdd_31, task_rdd_34, task_rdd_36, task_rdd_38, task_rdd_39, task_rdd_40])
    task_rdd_41 = simulate_task(union_rdd_41, '41', 3)

    union_rdd_32 = spark.sparkContext.union([task_rdd_46])
    task_rdd_32 = simulate_task(union_rdd_32, '32', 18)

    union_rdd_13 = spark.sparkContext.union([task_rdd_4, task_rdd_5, task_rdd_8, task_rdd_9, task_rdd_12])
    task_rdd_13 = simulate_task(union_rdd_13, '13', 20)

    union_rdd_26 = spark.sparkContext.union([task_rdd_28, task_rdd_30])
    task_rdd_26 = simulate_task(union_rdd_26, '26', 3)

    union_rdd_42 = spark.sparkContext.union([task_rdd_24, task_rdd_31, task_rdd_34, task_rdd_36, task_rdd_40, task_rdd_41])
    task_rdd_42 = simulate_task(union_rdd_42, '42', 5)

    union_rdd_37 = spark.sparkContext.union([task_rdd_31, task_rdd_32, task_rdd_36])
    task_rdd_37 = simulate_task(union_rdd_37, '37', 22)

    union_rdd_35 = spark.sparkContext.union([task_rdd_32, task_rdd_57])
    task_rdd_35 = simulate_task(union_rdd_35, '35', 117)

    union_rdd_25 = spark.sparkContext.union([task_rdd_24])
    task_rdd_25 = simulate_task(union_rdd_25, '25', 3)

    union_rdd_17 = spark.sparkContext.union([task_rdd_25, task_rdd_62])
    task_rdd_17 = simulate_task(union_rdd_17, '17', 526)

    combined_rdd = spark.sparkContext.union([task_rdd_13])
    task_rdd = simulate_task(combined_rdd, 'R50_13', 181)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
