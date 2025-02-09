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

    task_rdd_14 = simulate_task(base_rdd, '14', 1)
    task_rdd_11 = simulate_task(base_rdd, '11', 6)
    task_rdd_20 = simulate_task(base_rdd, '20', 6)
    task_rdd_18 = simulate_task(base_rdd, '18', 87)
    task_rdd_17 = simulate_task(base_rdd, '17', 6)
    task_rdd_16 = simulate_task(base_rdd, '16', 121)
    task_rdd_8 = simulate_task(base_rdd, '8', 3)
    task_rdd_7 = simulate_task(base_rdd, '7', 14)
    task_rdd_6 = simulate_task(base_rdd, '6', 91)
    task_rdd_3 = simulate_task(base_rdd, '3', 12)
    task_rdd_2 = simulate_task(base_rdd, '2', 36)
    task_rdd_5 = simulate_task(base_rdd, '5', 15)
    task_rdd_4 = simulate_task(base_rdd, '4', 2)
    task_rdd_9 = simulate_task(base_rdd, '9', 6)
    task_rdd_10 = simulate_task(base_rdd, '10', 46)
    task_rdd_1 = simulate_task(base_rdd, '1', 25)
    task_rdd_59 = simulate_task(base_rdd, '59', 15)
    task_rdd_42 = simulate_task(base_rdd, '42', 54)
    task_rdd_36 = simulate_task(base_rdd, '36', 19)
    task_rdd_32 = simulate_task(base_rdd, '32', 1)
    task_rdd_29 = simulate_task(base_rdd, '29', 2)
    task_rdd_27 = simulate_task(base_rdd, '27', 188)
    task_rdd_28 = simulate_task(base_rdd, '28', 76)
    task_rdd_34 = simulate_task(base_rdd, '34', 8)
    task_rdd_45 = simulate_task(base_rdd, '45', 10)
    task_rdd_46 = simulate_task(base_rdd, '46', 29)
    task_rdd_132 = simulate_task(base_rdd, '132', 4)
    task_rdd_100 = simulate_task(base_rdd, '100', 36)

    union_rdd_12 = spark.sparkContext.union([task_rdd_2, task_rdd_4, task_rdd_5, task_rdd_6, task_rdd_7, task_rdd_9, task_rdd_11])
    task_rdd_12 = simulate_task(union_rdd_12, '12', 8)

    union_rdd_21 = spark.sparkContext.union([task_rdd_1, task_rdd_2, task_rdd_3, task_rdd_5, task_rdd_6, task_rdd_7, task_rdd_9, task_rdd_20])
    task_rdd_21 = simulate_task(union_rdd_21, '21', 21)

    union_rdd_19 = spark.sparkContext.union([task_rdd_4, task_rdd_7, task_rdd_10, task_rdd_18])
    task_rdd_19 = simulate_task(union_rdd_19, '19', 198)

    union_rdd_15 = spark.sparkContext.union([task_rdd_1, task_rdd_2, task_rdd_4, task_rdd_5, task_rdd_6, task_rdd_7, task_rdd_8, task_rdd_9, task_rdd_10, task_rdd_14])
    task_rdd_15 = simulate_task(union_rdd_15, '15', 150)

    union_rdd_61 = spark.sparkContext.union([task_rdd_59])
    task_rdd_61 = simulate_task(union_rdd_61, '61', 1)

    union_rdd_35 = spark.sparkContext.union([task_rdd_34])
    task_rdd_35 = simulate_task(union_rdd_35, '35', 24)

    union_rdd_33 = spark.sparkContext.union([task_rdd_29, task_rdd_32])
    task_rdd_33 = simulate_task(union_rdd_33, '33', 112)

    union_rdd_30 = spark.sparkContext.union([task_rdd_28])
    task_rdd_30 = simulate_task(union_rdd_30, '30', 6)

    union_rdd_41 = spark.sparkContext.union([task_rdd_45])
    task_rdd_41 = simulate_task(union_rdd_41, '41', 1)

    union_rdd_47 = spark.sparkContext.union([task_rdd_46])
    task_rdd_47 = simulate_task(union_rdd_47, '47', 189)

    union_rdd_99 = spark.sparkContext.union([task_rdd_132])
    task_rdd_99 = simulate_task(union_rdd_99, '99', 4)

    union_rdd_53 = spark.sparkContext.union([task_rdd_100])
    task_rdd_53 = simulate_task(union_rdd_53, '53', 15)

    union_rdd_13 = spark.sparkContext.union([task_rdd_7, task_rdd_8, task_rdd_9, task_rdd_10, task_rdd_11, task_rdd_12])
    task_rdd_13 = simulate_task(union_rdd_13, '13', 16)

    union_rdd_22 = spark.sparkContext.union([task_rdd_3, task_rdd_5, task_rdd_17, task_rdd_21])
    task_rdd_22 = simulate_task(union_rdd_22, '22', 47)

    union_rdd_62 = spark.sparkContext.union([task_rdd_61])
    task_rdd_62 = simulate_task(union_rdd_62, '62', 5)

    union_rdd_31 = spark.sparkContext.union([task_rdd_30])
    task_rdd_31 = simulate_task(union_rdd_31, '31', 5)

    union_rdd_23 = spark.sparkContext.union([task_rdd_3, task_rdd_4, task_rdd_14, task_rdd_22])
    task_rdd_23 = simulate_task(union_rdd_23, '23', 9)

    union_rdd_37 = spark.sparkContext.union([task_rdd_31, task_rdd_36])
    task_rdd_37 = simulate_task(union_rdd_37, '37', 18)

    union_rdd_24 = spark.sparkContext.union([task_rdd_8, task_rdd_10, task_rdd_14, task_rdd_16, task_rdd_18, task_rdd_20, task_rdd_23])
    task_rdd_24 = simulate_task(union_rdd_24, '24', 63)

    union_rdd_39 = spark.sparkContext.union([task_rdd_35, task_rdd_36, task_rdd_37])
    task_rdd_39 = simulate_task(union_rdd_39, '39', 27)

    union_rdd_25 = spark.sparkContext.union([task_rdd_1, task_rdd_3, task_rdd_4, task_rdd_8, task_rdd_24])
    task_rdd_25 = simulate_task(union_rdd_25, '25', 119)

    union_rdd_40 = spark.sparkContext.union([task_rdd_34, task_rdd_39])
    task_rdd_40 = simulate_task(union_rdd_40, '40', 648)

    union_rdd_43 = spark.sparkContext.union([task_rdd_40, task_rdd_42])
    task_rdd_43 = simulate_task(union_rdd_43, '43', 1075)

    union_rdd_44 = spark.sparkContext.union([task_rdd_41, task_rdd_43])
    task_rdd_44 = simulate_task(union_rdd_44, '44', 43)

    union_rdd_63 = spark.sparkContext.union([task_rdd_44, task_rdd_62])
    task_rdd_63 = simulate_task(union_rdd_63, '63', 30)

    union_rdd_38 = spark.sparkContext.union([task_rdd_63])
    task_rdd_38 = simulate_task(union_rdd_38, '38', 4)

    combined_rdd = spark.sparkContext.union([task_rdd_4, task_rdd_9, task_rdd_11, task_rdd_13])
    task_rdd = simulate_task(combined_rdd, 'J14_4_9_11_13', 59)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
