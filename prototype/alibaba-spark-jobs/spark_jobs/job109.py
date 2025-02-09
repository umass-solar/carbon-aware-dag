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

    task_rdd_22 = simulate_task(base_rdd, '22', 47)
    task_rdd_56 = simulate_task(base_rdd, '56', 32)
    task_rdd_11 = simulate_task(base_rdd, '11', 21)
    task_rdd_10 = simulate_task(base_rdd, '10', 30)
    task_rdd_9 = simulate_task(base_rdd, '9', 3)
    task_rdd_8 = simulate_task(base_rdd, '8', 6)
    task_rdd_7 = simulate_task(base_rdd, '7', 2)
    task_rdd_6 = simulate_task(base_rdd, '6', 3)
    task_rdd_116 = simulate_task(base_rdd, '116', 5)
    task_rdd_5 = simulate_task(base_rdd, '5', 7)
    task_rdd_17 = simulate_task(base_rdd, '17', 10)
    task_rdd_16 = simulate_task(base_rdd, '16', 15)
    task_rdd_15 = simulate_task(base_rdd, '15', 6)
    task_rdd_47 = simulate_task(base_rdd, '47', 8)
    task_rdd_13 = simulate_task(base_rdd, '13', 96)
    task_rdd_2 = simulate_task(base_rdd, '2', 32)
    task_rdd_3 = simulate_task(base_rdd, '3', 534)
    task_rdd_52 = simulate_task(base_rdd, '52', 15)
    task_rdd_49 = simulate_task(base_rdd, '49', 11)
    task_rdd_1 = simulate_task(base_rdd, '1', 9)
    task_rdd_25 = simulate_task(base_rdd, '25', 75)
    task_rdd_45 = simulate_task(base_rdd, '45', 44)
    task_rdd_42 = simulate_task(base_rdd, '42', 40)
    task_rdd_75 = simulate_task(base_rdd, '75', 5)
    task_rdd_30 = simulate_task(base_rdd, '30', 75)

    union_rdd_18 = spark.sparkContext.union([task_rdd_56])
    task_rdd_18 = simulate_task(union_rdd_18, '18', 4)

    union_rdd_12 = spark.sparkContext.union([task_rdd_6, task_rdd_9])
    task_rdd_12 = simulate_task(union_rdd_12, '12', 37)

    union_rdd_120 = spark.sparkContext.union([task_rdd_116])
    task_rdd_120 = simulate_task(union_rdd_120, '120', 72)

    union_rdd_48 = spark.sparkContext.union([task_rdd_2, task_rdd_13, task_rdd_47])
    task_rdd_48 = simulate_task(union_rdd_48, '48', 296)

    union_rdd_53 = spark.sparkContext.union([task_rdd_49, task_rdd_52])
    task_rdd_53 = simulate_task(union_rdd_53, '53', 118)

    union_rdd_28 = spark.sparkContext.union([task_rdd_22])
    task_rdd_28 = simulate_task(union_rdd_28, '28', 673)

    union_rdd_43 = spark.sparkContext.union([task_rdd_42, task_rdd_45])
    task_rdd_43 = simulate_task(union_rdd_43, '43', 15)

    union_rdd_32 = spark.sparkContext.union([task_rdd_30])
    task_rdd_32 = simulate_task(union_rdd_32, '32', 8)

    union_rdd_19 = spark.sparkContext.union([task_rdd_2, task_rdd_3, task_rdd_11, task_rdd_18])
    task_rdd_19 = simulate_task(union_rdd_19, '19', 266)

    union_rdd_14 = spark.sparkContext.union([task_rdd_120])
    task_rdd_14 = simulate_task(union_rdd_14, '14', 4)

    union_rdd_29 = spark.sparkContext.union([task_rdd_28, task_rdd_48])
    task_rdd_29 = simulate_task(union_rdd_29, '29', 37)

    union_rdd_23 = spark.sparkContext.union([task_rdd_43])
    task_rdd_23 = simulate_task(union_rdd_23, '23', 4)

    union_rdd_31 = spark.sparkContext.union([task_rdd_30, task_rdd_32])
    task_rdd_31 = simulate_task(union_rdd_31, '31', 25)

    union_rdd_4 = spark.sparkContext.union([task_rdd_1, task_rdd_19])
    task_rdd_4 = simulate_task(union_rdd_4, '4', 125)

    union_rdd_26 = spark.sparkContext.union([task_rdd_25, task_rdd_29])
    task_rdd_26 = simulate_task(union_rdd_26, '26', 49)

    union_rdd_54 = spark.sparkContext.union([task_rdd_31, task_rdd_75])
    task_rdd_54 = simulate_task(union_rdd_54, '54', 192)

    union_rdd_24 = spark.sparkContext.union([task_rdd_23])
    task_rdd_24 = simulate_task(union_rdd_24, '24', 1)

    union_rdd_20 = spark.sparkContext.union([task_rdd_1, task_rdd_2, task_rdd_3, task_rdd_4])
    task_rdd_20 = simulate_task(union_rdd_20, '20', 1)

    union_rdd_27 = spark.sparkContext.union([task_rdd_26])
    task_rdd_27 = simulate_task(union_rdd_27, '27', 6)

    union_rdd_55 = spark.sparkContext.union([task_rdd_53, task_rdd_54])
    task_rdd_55 = simulate_task(union_rdd_55, '55', 21)

    union_rdd_21 = spark.sparkContext.union([task_rdd_15, task_rdd_16, task_rdd_17, task_rdd_20])
    task_rdd_21 = simulate_task(union_rdd_21, '21', 6)

    union_rdd_34 = spark.sparkContext.union([task_rdd_27])
    task_rdd_34 = simulate_task(union_rdd_34, '34', 9)

    union_rdd_44 = spark.sparkContext.union([task_rdd_55])
    task_rdd_44 = simulate_task(union_rdd_44, '44', 13)

    union_rdd_35 = spark.sparkContext.union([task_rdd_25, task_rdd_34])
    task_rdd_35 = simulate_task(union_rdd_35, '35', 12)

    union_rdd_41 = spark.sparkContext.union([task_rdd_44])
    task_rdd_41 = simulate_task(union_rdd_41, '41', 2)

    union_rdd_57 = spark.sparkContext.union([task_rdd_35])
    task_rdd_57 = simulate_task(union_rdd_57, '57', 6)

    union_rdd_58 = spark.sparkContext.union([task_rdd_54, task_rdd_57])
    task_rdd_58 = simulate_task(union_rdd_58, '58', 144)

    combined_rdd = spark.sparkContext.union([task_rdd_8, task_rdd_10, task_rdd_11, task_rdd_21])
    task_rdd = simulate_task(combined_rdd, 'J22_8_10_11_21', 40)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
