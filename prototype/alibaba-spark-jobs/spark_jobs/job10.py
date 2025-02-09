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

    task_rdd_11 = simulate_task(base_rdd, '11', 10)
    task_rdd_10 = simulate_task(base_rdd, '10', 3)
    task_rdd_8 = simulate_task(base_rdd, '8', 25)
    task_rdd_6 = simulate_task(base_rdd, '6', 41)
    task_rdd_1 = simulate_task(base_rdd, '1', 1)
    task_rdd_4 = simulate_task(base_rdd, '4', 1)
    task_rdd_14 = simulate_task(base_rdd, '14', 3)
    task_rdd_3 = simulate_task(base_rdd, '3', 8)
    task_rdd_25 = simulate_task(base_rdd, '25', 2)
    task_rdd_24 = simulate_task(base_rdd, '24', 60)
    task_rdd_131 = simulate_task(base_rdd, '131', 1)
    task_rdd_55 = simulate_task(base_rdd, '55', 115)
    task_rdd_51 = simulate_task(base_rdd, '51', 167)
    task_rdd_21 = simulate_task(base_rdd, '21', 26)
    task_rdd_19 = simulate_task(base_rdd, '19', 239)
    task_rdd_18 = simulate_task(base_rdd, '18', 360)
    task_rdd_15 = simulate_task(base_rdd, '15', 23)
    task_rdd_29 = simulate_task(base_rdd, '29', 22)
    task_rdd_37 = simulate_task(base_rdd, '37', 6)
    task_rdd_47 = simulate_task(base_rdd, '47', 26)
    task_rdd_33 = simulate_task(base_rdd, '33', 1)

    union_rdd_2 = spark.sparkContext.union([task_rdd_33, task_rdd_47])
    task_rdd_2 = simulate_task(union_rdd_2, '2', 3)

    union_rdd_26 = spark.sparkContext.union([task_rdd_15, task_rdd_19, task_rdd_24, task_rdd_25])
    task_rdd_26 = simulate_task(union_rdd_26, '26', 66)

    union_rdd_22 = spark.sparkContext.union([task_rdd_18, task_rdd_19, task_rdd_21])
    task_rdd_22 = simulate_task(union_rdd_22, '22', 1236)

    union_rdd_132 = spark.sparkContext.union([task_rdd_131])
    task_rdd_132 = simulate_task(union_rdd_132, '132', 1)

    union_rdd_45 = spark.sparkContext.union([task_rdd_51])
    task_rdd_45 = simulate_task(union_rdd_45, '45', 2)

    union_rdd_30 = spark.sparkContext.union([task_rdd_29])
    task_rdd_30 = simulate_task(union_rdd_30, '30', 241)

    union_rdd_34 = spark.sparkContext.union([task_rdd_37])
    task_rdd_34 = simulate_task(union_rdd_34, '34', 21)

    union_rdd_16 = spark.sparkContext.union([task_rdd_2, task_rdd_8])
    task_rdd_16 = simulate_task(union_rdd_16, '16', 5)

    union_rdd_7 = spark.sparkContext.union([task_rdd_2, task_rdd_4, task_rdd_6])
    task_rdd_7 = simulate_task(union_rdd_7, '7', 663)

    union_rdd_5 = spark.sparkContext.union([task_rdd_34])
    task_rdd_5 = simulate_task(union_rdd_5, '5', 1)

    union_rdd_9 = spark.sparkContext.union([task_rdd_15, task_rdd_19, task_rdd_25, task_rdd_26])
    task_rdd_9 = simulate_task(union_rdd_9, '9', 51)

    union_rdd_23 = spark.sparkContext.union([task_rdd_22])
    task_rdd_23 = simulate_task(union_rdd_23, '23', 103)

    union_rdd_101 = spark.sparkContext.union([task_rdd_132])
    task_rdd_101 = simulate_task(union_rdd_101, '101', 4)

    union_rdd_12 = spark.sparkContext.union([task_rdd_2, task_rdd_4, task_rdd_6, task_rdd_9, task_rdd_10, task_rdd_11])
    task_rdd_12 = simulate_task(union_rdd_12, '12', 42)

    union_rdd_17 = spark.sparkContext.union([task_rdd_2, task_rdd_4, task_rdd_6, task_rdd_16])
    task_rdd_17 = simulate_task(union_rdd_17, '17', 412)

    union_rdd_100 = spark.sparkContext.union([task_rdd_101])
    task_rdd_100 = simulate_task(union_rdd_100, '100', 2)

    union_rdd_13 = spark.sparkContext.union([task_rdd_1, task_rdd_2, task_rdd_4, task_rdd_5, task_rdd_11, task_rdd_12])
    task_rdd_13 = simulate_task(union_rdd_13, '13', 297)

    union_rdd_20 = spark.sparkContext.union([task_rdd_17])
    task_rdd_20 = simulate_task(union_rdd_20, '20', 25)

    union_rdd_61 = spark.sparkContext.union([task_rdd_100])
    task_rdd_61 = simulate_task(union_rdd_61, '61', 14)

    union_rdd_39 = spark.sparkContext.union([task_rdd_13])
    task_rdd_39 = simulate_task(union_rdd_39, '39', 17)

    union_rdd_62 = spark.sparkContext.union([task_rdd_55, task_rdd_61])
    task_rdd_62 = simulate_task(union_rdd_62, '62', 43)

    combined_rdd = spark.sparkContext.union([task_rdd_6, task_rdd_39])
    task_rdd = simulate_task(combined_rdd, 'J40_6_39', 18)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
