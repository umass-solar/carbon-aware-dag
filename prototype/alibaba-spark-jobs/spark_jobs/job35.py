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

    task_rdd_3 = simulate_task(base_rdd, '3', 24)
    task_rdd_27 = simulate_task(base_rdd, '27', 197)
    task_rdd_6 = simulate_task(base_rdd, '6', 3)
    task_rdd_36 = simulate_task(base_rdd, '36', 14)
    task_rdd_5 = simulate_task(base_rdd, '5', 7)
    task_rdd_2 = simulate_task(base_rdd, '2', 48)
    task_rdd_13 = simulate_task(base_rdd, '13', 2)
    task_rdd_1 = simulate_task(base_rdd, '1', 2)
    task_rdd_40 = simulate_task(base_rdd, '40', 100)
    task_rdd_39 = simulate_task(base_rdd, '39', 12)
    task_rdd_97 = simulate_task(base_rdd, '97', 11)
    task_rdd_20 = simulate_task(base_rdd, '20', 5)
    task_rdd_19 = simulate_task(base_rdd, '19', 8)
    task_rdd_15 = simulate_task(base_rdd, '15', 73)
    task_rdd_14 = simulate_task(base_rdd, '14', 3)
    task_rdd_57 = simulate_task(base_rdd, '57', 13)
    task_rdd_12 = simulate_task(base_rdd, '12', 5)
    task_rdd_4 = simulate_task(base_rdd, '4', 1)
    task_rdd_31 = simulate_task(base_rdd, '31', 137)
    task_rdd_28 = simulate_task(base_rdd, '28', 39)
    task_rdd_22 = simulate_task(base_rdd, '22', 68)
    task_rdd_21 = simulate_task(base_rdd, '21', 37)
    task_rdd_45 = simulate_task(base_rdd, '45', 100)
    task_rdd_131 = simulate_task(base_rdd, '131', 4)
    task_rdd_42 = simulate_task(base_rdd, '42', 152)

    union_rdd_49 = spark.sparkContext.union([task_rdd_27])
    task_rdd_49 = simulate_task(union_rdd_49, '49', 16)

    union_rdd_37 = spark.sparkContext.union([task_rdd_36])
    task_rdd_37 = simulate_task(union_rdd_37, '37', 5)

    union_rdd_34 = spark.sparkContext.union([task_rdd_5])
    task_rdd_34 = simulate_task(union_rdd_34, '34', 26)

    union_rdd_18 = spark.sparkContext.union([task_rdd_1, task_rdd_13])
    task_rdd_18 = simulate_task(union_rdd_18, '18', 22)

    union_rdd_41 = spark.sparkContext.union([task_rdd_22])
    task_rdd_41 = simulate_task(union_rdd_41, '41', 20)

    union_rdd_98 = spark.sparkContext.union([task_rdd_97])
    task_rdd_98 = simulate_task(union_rdd_98, '98', 704)

    union_rdd_43 = spark.sparkContext.union([task_rdd_42])
    task_rdd_43 = simulate_task(union_rdd_43, '43', 7)

    union_rdd_132 = spark.sparkContext.union([task_rdd_131])
    task_rdd_132 = simulate_task(union_rdd_132, '132', 1)

    union_rdd_10 = spark.sparkContext.union([task_rdd_37])
    task_rdd_10 = simulate_task(union_rdd_10, '10', 4)

    union_rdd_35 = spark.sparkContext.union([task_rdd_12, task_rdd_19, task_rdd_21, task_rdd_28, task_rdd_34])
    task_rdd_35 = simulate_task(union_rdd_35, '35', 2)

    union_rdd_44 = spark.sparkContext.union([task_rdd_132])
    task_rdd_44 = simulate_task(union_rdd_44, '44', 13)

    union_rdd_55 = spark.sparkContext.union([task_rdd_15, task_rdd_28, task_rdd_41])
    task_rdd_55 = simulate_task(union_rdd_55, '55', 11)

    union_rdd_99 = spark.sparkContext.union([task_rdd_98])
    task_rdd_99 = simulate_task(union_rdd_99, '99', 14)

    union_rdd_50 = spark.sparkContext.union([task_rdd_27, task_rdd_36, task_rdd_43, task_rdd_44, task_rdd_49])
    task_rdd_50 = simulate_task(union_rdd_50, '50', 72)

    union_rdd_11 = spark.sparkContext.union([task_rdd_2, task_rdd_4, task_rdd_10])
    task_rdd_11 = simulate_task(union_rdd_11, '11', 15)

    union_rdd_51 = spark.sparkContext.union([task_rdd_3, task_rdd_4, task_rdd_12, task_rdd_14, task_rdd_40, task_rdd_41, task_rdd_43, task_rdd_44])
    task_rdd_51 = simulate_task(union_rdd_51, '51', 292)

    union_rdd_59 = spark.sparkContext.union([task_rdd_99])
    task_rdd_59 = simulate_task(union_rdd_59, '59', 10)

    union_rdd_33 = spark.sparkContext.union([task_rdd_50])
    task_rdd_33 = simulate_task(union_rdd_33, '33', 4)

    union_rdd_38 = spark.sparkContext.union([task_rdd_51])
    task_rdd_38 = simulate_task(union_rdd_38, '38', 133)

    union_rdd_26 = spark.sparkContext.union([task_rdd_33])
    task_rdd_26 = simulate_task(union_rdd_26, '26', 6)

    union_rdd_30 = spark.sparkContext.union([task_rdd_26])
    task_rdd_30 = simulate_task(union_rdd_30, '30', 203)

    union_rdd_16 = spark.sparkContext.union([task_rdd_30])
    task_rdd_16 = simulate_task(union_rdd_16, '16', 1258)

    union_rdd_17 = spark.sparkContext.union([task_rdd_2, task_rdd_4, task_rdd_12, task_rdd_16])
    task_rdd_17 = simulate_task(union_rdd_17, '17', 55)

    union_rdd_29 = spark.sparkContext.union([task_rdd_17])
    task_rdd_29 = simulate_task(union_rdd_29, '29', 128)

    union_rdd_7 = spark.sparkContext.union([task_rdd_5, task_rdd_6, task_rdd_19, task_rdd_29])
    task_rdd_7 = simulate_task(union_rdd_7, '7', 47)

    union_rdd_8 = spark.sparkContext.union([task_rdd_1, task_rdd_5, task_rdd_7])
    task_rdd_8 = simulate_task(union_rdd_8, '8', 55)

    union_rdd_9 = spark.sparkContext.union([task_rdd_4, task_rdd_6, task_rdd_8])
    task_rdd_9 = simulate_task(union_rdd_9, '9', 16)

    combined_rdd = spark.sparkContext.union([task_rdd_9])
    task_rdd = simulate_task(combined_rdd, 'M3_9', 5)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
