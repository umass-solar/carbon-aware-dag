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

    task_rdd_12 = simulate_task(base_rdd, '12', 4)
    task_rdd_6 = simulate_task(base_rdd, '6', 1)
    task_rdd_9 = simulate_task(base_rdd, '9', 5)
    task_rdd_8 = simulate_task(base_rdd, '8', 8)
    task_rdd_7 = simulate_task(base_rdd, '7', 2)
    task_rdd_4 = simulate_task(base_rdd, '4', 23)
    task_rdd_3 = simulate_task(base_rdd, '3', 167)
    task_rdd_5 = simulate_task(base_rdd, '5', 3)
    task_rdd_2 = simulate_task(base_rdd, '2', 9)
    task_rdd_49 = simulate_task(base_rdd, '49', 9)
    task_rdd_28 = simulate_task(base_rdd, '28', 8)
    task_rdd_24 = simulate_task(base_rdd, '24', 1)
    task_rdd_23 = simulate_task(base_rdd, '23', 43)
    task_rdd_1 = simulate_task(base_rdd, '1', 151)
    task_rdd_120 = simulate_task(base_rdd, '120', 36)
    task_rdd_31 = simulate_task(base_rdd, '31', 3)
    task_rdd_70 = simulate_task(base_rdd, '70', 172)
    task_rdd_34 = simulate_task(base_rdd, '34', 26)
    task_rdd_37 = simulate_task(base_rdd, '37', 183)
    task_rdd_30 = simulate_task(base_rdd, '30', 79)
    task_rdd_29 = simulate_task(base_rdd, '29', 7)

    union_rdd_10 = spark.sparkContext.union([task_rdd_3, task_rdd_6])
    task_rdd_10 = simulate_task(union_rdd_10, '10', 65)

    union_rdd_13 = spark.sparkContext.union([task_rdd_1, task_rdd_5, task_rdd_8])
    task_rdd_13 = simulate_task(union_rdd_13, '13', 6)

    union_rdd_47 = spark.sparkContext.union([task_rdd_5])
    task_rdd_47 = simulate_task(union_rdd_47, '47', 2)

    union_rdd_44 = spark.sparkContext.union([task_rdd_49])
    task_rdd_44 = simulate_task(union_rdd_44, '44', 71)

    union_rdd_25 = spark.sparkContext.union([task_rdd_23, task_rdd_24])
    task_rdd_25 = simulate_task(union_rdd_25, '25', 7)

    union_rdd_16 = spark.sparkContext.union([task_rdd_1, task_rdd_12])
    task_rdd_16 = simulate_task(union_rdd_16, '16', 55)

    union_rdd_40 = spark.sparkContext.union([task_rdd_120])
    task_rdd_40 = simulate_task(union_rdd_40, '40', 4)

    union_rdd_32 = spark.sparkContext.union([task_rdd_31])
    task_rdd_32 = simulate_task(union_rdd_32, '32', 3)

    union_rdd_69 = spark.sparkContext.union([task_rdd_70])
    task_rdd_69 = simulate_task(union_rdd_69, '69', 1)

    union_rdd_33 = spark.sparkContext.union([task_rdd_29])
    task_rdd_33 = simulate_task(union_rdd_33, '33', 61)

    union_rdd_38 = spark.sparkContext.union([task_rdd_37])
    task_rdd_38 = simulate_task(union_rdd_38, '38', 47)

    union_rdd_11 = spark.sparkContext.union([task_rdd_1, task_rdd_4, task_rdd_8, task_rdd_9, task_rdd_10])
    task_rdd_11 = simulate_task(union_rdd_11, '11', 155)

    union_rdd_14 = spark.sparkContext.union([task_rdd_2, task_rdd_3, task_rdd_4, task_rdd_5, task_rdd_13])
    task_rdd_14 = simulate_task(union_rdd_14, '14', 301)

    union_rdd_26 = spark.sparkContext.union([task_rdd_24, task_rdd_25])
    task_rdd_26 = simulate_task(union_rdd_26, '26', 210)

    union_rdd_17 = spark.sparkContext.union([task_rdd_16])
    task_rdd_17 = simulate_task(union_rdd_17, '17', 3)

    union_rdd_41 = spark.sparkContext.union([task_rdd_32, task_rdd_40])
    task_rdd_41 = simulate_task(union_rdd_41, '41', 123)

    union_rdd_36 = spark.sparkContext.union([task_rdd_69])
    task_rdd_36 = simulate_task(union_rdd_36, '36', 7)

    union_rdd_39 = spark.sparkContext.union([task_rdd_30, task_rdd_38])
    task_rdd_39 = simulate_task(union_rdd_39, '39', 136)

    union_rdd_15 = spark.sparkContext.union([task_rdd_14])
    task_rdd_15 = simulate_task(union_rdd_15, '15', 6)

    union_rdd_27 = spark.sparkContext.union([task_rdd_24, task_rdd_26])
    task_rdd_27 = simulate_task(union_rdd_27, '27', 1)

    union_rdd_18 = spark.sparkContext.union([task_rdd_12, task_rdd_17])
    task_rdd_18 = simulate_task(union_rdd_18, '18', 35)

    union_rdd_42 = spark.sparkContext.union([task_rdd_34, task_rdd_36, task_rdd_41])
    task_rdd_42 = simulate_task(union_rdd_42, '42', 20)

    union_rdd_21 = spark.sparkContext.union([task_rdd_2, task_rdd_8, task_rdd_15])
    task_rdd_21 = simulate_task(union_rdd_21, '21', 165)

    union_rdd_48 = spark.sparkContext.union([task_rdd_27, task_rdd_28, task_rdd_47])
    task_rdd_48 = simulate_task(union_rdd_48, '48', 191)

    union_rdd_45 = spark.sparkContext.union([task_rdd_37, task_rdd_39, task_rdd_42])
    task_rdd_45 = simulate_task(union_rdd_45, '45', 221)

    union_rdd_22 = spark.sparkContext.union([task_rdd_3, task_rdd_4, task_rdd_21])
    task_rdd_22 = simulate_task(union_rdd_22, '22', 136)

    union_rdd_46 = spark.sparkContext.union([task_rdd_45])
    task_rdd_46 = simulate_task(union_rdd_46, '46', 133)

    union_rdd_35 = spark.sparkContext.union([task_rdd_33, task_rdd_34, task_rdd_46])
    task_rdd_35 = simulate_task(union_rdd_35, '35', 62)

    union_rdd_19 = spark.sparkContext.union([task_rdd_35])
    task_rdd_19 = simulate_task(union_rdd_19, '19', 30)

    union_rdd_20 = spark.sparkContext.union([task_rdd_12, task_rdd_19])
    task_rdd_20 = simulate_task(union_rdd_20, '20', 4)

    combined_rdd = spark.sparkContext.union([task_rdd_6, task_rdd_8, task_rdd_11])
    task_rdd = simulate_task(combined_rdd, 'J12_6_8_11', 86)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
