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

    task_rdd_15 = simulate_task(base_rdd, '15', 14)
    task_rdd_14 = simulate_task(base_rdd, '14', 15)
    task_rdd_24 = simulate_task(base_rdd, '24', 7)
    task_rdd_21 = simulate_task(base_rdd, '21', 10)
    task_rdd_11 = simulate_task(base_rdd, '11', 10)
    task_rdd_129 = simulate_task(base_rdd, '129', 4)
    task_rdd_7 = simulate_task(base_rdd, '7', 64)
    task_rdd_9 = simulate_task(base_rdd, '9', 13)
    task_rdd_32 = simulate_task(base_rdd, '32', 46)
    task_rdd_30 = simulate_task(base_rdd, '30', 61)
    task_rdd_10 = simulate_task(base_rdd, '10', 20)
    task_rdd_8 = simulate_task(base_rdd, '8', 33)
    task_rdd_4 = simulate_task(base_rdd, '4', 4)
    task_rdd_3 = simulate_task(base_rdd, '3', 26)
    task_rdd_2 = simulate_task(base_rdd, '2', 189)
    task_rdd_1 = simulate_task(base_rdd, '1', 16)
    task_rdd_13 = simulate_task(base_rdd, '13', 2)
    task_rdd_12 = simulate_task(base_rdd, '12', 5)
    task_rdd_5 = simulate_task(base_rdd, '5', 3)
    task_rdd_26 = simulate_task(base_rdd, '26', 1)
    task_rdd_36 = simulate_task(base_rdd, '36', 79)
    task_rdd_42 = simulate_task(base_rdd, '42', 40)
    task_rdd_56 = simulate_task(base_rdd, '56', 7)
    task_rdd_69 = simulate_task(base_rdd, '69', 8)
    task_rdd_40 = simulate_task(base_rdd, '40', 20)
    task_rdd_39 = simulate_task(base_rdd, '39', 140)
    task_rdd_54 = simulate_task(base_rdd, '54', 11)
    task_rdd_53 = simulate_task(base_rdd, '53', 12)
    task_rdd_38 = simulate_task(base_rdd, '38', 42)
    task_rdd_61 = simulate_task(base_rdd, '61', 117)

    union_rdd_16 = spark.sparkContext.union([task_rdd_4, task_rdd_7, task_rdd_8, task_rdd_9, task_rdd_15])
    task_rdd_16 = simulate_task(union_rdd_16, '16', 277)

    union_rdd_25 = spark.sparkContext.union([task_rdd_1, task_rdd_2, task_rdd_3, task_rdd_4, task_rdd_24])
    task_rdd_25 = simulate_task(union_rdd_25, '25', 178)

    union_rdd_22 = spark.sparkContext.union([task_rdd_2, task_rdd_4, task_rdd_8, task_rdd_11, task_rdd_21])
    task_rdd_22 = simulate_task(union_rdd_22, '22', 5)

    union_rdd_130 = spark.sparkContext.union([task_rdd_7, task_rdd_129])
    task_rdd_130 = simulate_task(union_rdd_130, '130', 12)

    union_rdd_6 = spark.sparkContext.union([task_rdd_61])
    task_rdd_6 = simulate_task(union_rdd_6, '6', 12)

    union_rdd_33 = spark.sparkContext.union([task_rdd_32])
    task_rdd_33 = simulate_task(union_rdd_33, '33', 38)

    union_rdd_31 = spark.sparkContext.union([task_rdd_30])
    task_rdd_31 = simulate_task(union_rdd_31, '31', 12)

    union_rdd_37 = spark.sparkContext.union([task_rdd_36])
    task_rdd_37 = simulate_task(union_rdd_37, '37', 11)

    union_rdd_43 = spark.sparkContext.union([task_rdd_39])
    task_rdd_43 = simulate_task(union_rdd_43, '43', 6)

    union_rdd_34 = spark.sparkContext.union([task_rdd_56])
    task_rdd_34 = simulate_task(union_rdd_34, '34', 5)

    union_rdd_70 = spark.sparkContext.union([task_rdd_40, task_rdd_69])
    task_rdd_70 = simulate_task(union_rdd_70, '70', 488)

    union_rdd_18 = spark.sparkContext.union([task_rdd_13])
    task_rdd_18 = simulate_task(union_rdd_18, '18', 117)

    union_rdd_132 = spark.sparkContext.union([])
    task_rdd_132 = simulate_task(union_rdd_132, '132', 1)

    union_rdd_28 = spark.sparkContext.union([task_rdd_26])
    task_rdd_28 = simulate_task(union_rdd_28, '28', 1)

    union_rdd_48 = spark.sparkContext.union([task_rdd_37])
    task_rdd_48 = simulate_task(union_rdd_48, '48', 62)

    union_rdd_74 = spark.sparkContext.union([])
    task_rdd_74 = simulate_task(union_rdd_74, '74', 1676)

    union_rdd_19 = spark.sparkContext.union([task_rdd_3, task_rdd_6, task_rdd_9, task_rdd_18])
    task_rdd_19 = simulate_task(union_rdd_19, '19', 2594)

    union_rdd_62 = spark.sparkContext.union([task_rdd_132])
    task_rdd_62 = simulate_task(union_rdd_62, '62', 4)

    union_rdd_29 = spark.sparkContext.union([task_rdd_2, task_rdd_4, task_rdd_8, task_rdd_10, task_rdd_28])
    task_rdd_29 = simulate_task(union_rdd_29, '29', 20)

    union_rdd_75 = spark.sparkContext.union([task_rdd_74])
    task_rdd_75 = simulate_task(union_rdd_75, '75', 4)

    union_rdd_20 = spark.sparkContext.union([task_rdd_11, task_rdd_13, task_rdd_19])
    task_rdd_20 = simulate_task(union_rdd_20, '20', 363)

    union_rdd_71 = spark.sparkContext.union([task_rdd_75])
    task_rdd_71 = simulate_task(union_rdd_71, '71', 6)

    union_rdd_72 = spark.sparkContext.union([task_rdd_71])
    task_rdd_72 = simulate_task(union_rdd_72, '72', 27)

    union_rdd_41 = spark.sparkContext.union([task_rdd_72])
    task_rdd_41 = simulate_task(union_rdd_41, '41', 2)

    union_rdd_44 = spark.sparkContext.union([task_rdd_41])
    task_rdd_44 = simulate_task(union_rdd_44, '44', 4)

    union_rdd_45 = spark.sparkContext.union([task_rdd_44, task_rdd_48])
    task_rdd_45 = simulate_task(union_rdd_45, '45', 192)

    union_rdd_46 = spark.sparkContext.union([task_rdd_45, task_rdd_62])
    task_rdd_46 = simulate_task(union_rdd_46, '46', 212)

    union_rdd_47 = spark.sparkContext.union([task_rdd_30, task_rdd_36, task_rdd_39, task_rdd_43, task_rdd_44, task_rdd_46])
    task_rdd_47 = simulate_task(union_rdd_47, '47', 95)

    combined_rdd = spark.sparkContext.union([task_rdd_1, task_rdd_2, task_rdd_3, task_rdd_4, task_rdd_5, task_rdd_6, task_rdd_7, task_rdd_8, task_rdd_9, task_rdd_10, task_rdd_11, task_rdd_12, task_rdd_13, task_rdd_14, task_rdd_15, task_rdd_16])
    task_rdd = simulate_task(combined_rdd, 'R17_1_2_3_4_5_6_7_8_9_10_11_12_13_14_15_16', 70)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
