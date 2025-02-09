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

    task_rdd_32 = simulate_task(base_rdd, '32', 4)
    task_rdd_78 = simulate_task(base_rdd, '78', 8)
    task_rdd_8 = simulate_task(base_rdd, '8', 115)
    task_rdd_17 = simulate_task(base_rdd, '17', 10)
    task_rdd_13 = simulate_task(base_rdd, '13', 1)
    task_rdd_12 = simulate_task(base_rdd, '12', 30)
    task_rdd_23 = simulate_task(base_rdd, '23', 83)
    task_rdd_6 = simulate_task(base_rdd, '6', 4)
    task_rdd_16 = simulate_task(base_rdd, '16', 25)
    task_rdd_132 = simulate_task(base_rdd, '132', 4)
    task_rdd_98 = simulate_task(base_rdd, '98', 4)
    task_rdd_3 = simulate_task(base_rdd, '3', 3)
    task_rdd_11 = simulate_task(base_rdd, '11', 8)
    task_rdd_1 = simulate_task(base_rdd, '1', 1)
    task_rdd_27 = simulate_task(base_rdd, '27', 34)
    task_rdd_20 = simulate_task(base_rdd, '20', 3)
    task_rdd_46 = simulate_task(base_rdd, '46', 148)
    task_rdd_29 = simulate_task(base_rdd, '29', 2)
    task_rdd_26 = simulate_task(base_rdd, '26', 3)
    task_rdd_24 = simulate_task(base_rdd, '24', 100)
    task_rdd_60 = simulate_task(base_rdd, '60', 4)
    task_rdd_30 = simulate_task(base_rdd, '30', 42)

    union_rdd_33 = spark.sparkContext.union([task_rdd_32])
    task_rdd_33 = simulate_task(union_rdd_33, '33', 162)

    union_rdd_47 = spark.sparkContext.union([task_rdd_46])
    task_rdd_47 = simulate_task(union_rdd_47, '47', 2)

    union_rdd_22 = spark.sparkContext.union([task_rdd_8])
    task_rdd_22 = simulate_task(union_rdd_22, '22', 22)

    union_rdd_25 = spark.sparkContext.union([task_rdd_60])
    task_rdd_25 = simulate_task(union_rdd_25, '25', 128)

    union_rdd_9 = spark.sparkContext.union([task_rdd_30])
    task_rdd_9 = simulate_task(union_rdd_9, '9', 134)

    union_rdd_99 = spark.sparkContext.union([task_rdd_132])
    task_rdd_99 = simulate_task(union_rdd_99, '99', 4)

    union_rdd_2 = spark.sparkContext.union([task_rdd_1, task_rdd_11])
    task_rdd_2 = simulate_task(union_rdd_2, '2', 70)

    union_rdd_21 = spark.sparkContext.union([task_rdd_20])
    task_rdd_21 = simulate_task(union_rdd_21, '21', 5)

    union_rdd_61 = spark.sparkContext.union([task_rdd_24])
    task_rdd_61 = simulate_task(union_rdd_61, '61', 15)

    union_rdd_34 = spark.sparkContext.union([task_rdd_12, task_rdd_32, task_rdd_33])
    task_rdd_34 = simulate_task(union_rdd_34, '34', 1623)

    union_rdd_63 = spark.sparkContext.union([task_rdd_11, task_rdd_12, task_rdd_22, task_rdd_47])
    task_rdd_63 = simulate_task(union_rdd_63, '63', 22)

    union_rdd_4 = spark.sparkContext.union([task_rdd_3, task_rdd_9, task_rdd_16])
    task_rdd_4 = simulate_task(union_rdd_4, '4', 31)

    union_rdd_100 = spark.sparkContext.union([task_rdd_98, task_rdd_99])
    task_rdd_100 = simulate_task(union_rdd_100, '100', 97)

    union_rdd_28 = spark.sparkContext.union([task_rdd_21, task_rdd_27])
    task_rdd_28 = simulate_task(union_rdd_28, '28', 576)

    union_rdd_10 = spark.sparkContext.union([task_rdd_8, task_rdd_9, task_rdd_24, task_rdd_25, task_rdd_34])
    task_rdd_10 = simulate_task(union_rdd_10, '10', 256)

    union_rdd_64 = spark.sparkContext.union([task_rdd_9, task_rdd_13, task_rdd_17, task_rdd_26, task_rdd_29, task_rdd_47, task_rdd_63])
    task_rdd_64 = simulate_task(union_rdd_64, '64', 400)

    union_rdd_14 = spark.sparkContext.union([task_rdd_4, task_rdd_6, task_rdd_13])
    task_rdd_14 = simulate_task(union_rdd_14, '14', 45)

    union_rdd_7 = spark.sparkContext.union([task_rdd_100])
    task_rdd_7 = simulate_task(union_rdd_7, '7', 14)

    union_rdd_44 = spark.sparkContext.union([task_rdd_10])
    task_rdd_44 = simulate_task(union_rdd_44, '44', 1110)

    union_rdd_62 = spark.sparkContext.union([task_rdd_61, task_rdd_64])
    task_rdd_62 = simulate_task(union_rdd_62, '62', 83)

    union_rdd_15 = spark.sparkContext.union([task_rdd_1, task_rdd_3, task_rdd_6, task_rdd_14])
    task_rdd_15 = simulate_task(union_rdd_15, '15', 345)

    union_rdd_45 = spark.sparkContext.union([task_rdd_44])
    task_rdd_45 = simulate_task(union_rdd_45, '45', 77)

    union_rdd_42 = spark.sparkContext.union([task_rdd_62])
    task_rdd_42 = simulate_task(union_rdd_42, '42', 54)

    union_rdd_35 = spark.sparkContext.union([task_rdd_45])
    task_rdd_35 = simulate_task(union_rdd_35, '35', 293)

    union_rdd_36 = spark.sparkContext.union([task_rdd_12, task_rdd_13, task_rdd_35])
    task_rdd_36 = simulate_task(union_rdd_36, '36', 92)

    union_rdd_18 = spark.sparkContext.union([task_rdd_36])
    task_rdd_18 = simulate_task(union_rdd_18, '18', 152)

    union_rdd_19 = spark.sparkContext.union([task_rdd_1, task_rdd_17, task_rdd_18])
    task_rdd_19 = simulate_task(union_rdd_19, '19', 235)

    union_rdd_5 = spark.sparkContext.union([task_rdd_19])
    task_rdd_5 = simulate_task(union_rdd_5, '5', 1)

    union_rdd_87 = spark.sparkContext.union([task_rdd_5])
    task_rdd_87 = simulate_task(union_rdd_87, '87', 3)

    combined_rdd = spark.sparkContext.union([task_rdd_8, task_rdd_16, task_rdd_87])
    task_rdd = simulate_task(combined_rdd, 'J83_8_16_87', 7)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
