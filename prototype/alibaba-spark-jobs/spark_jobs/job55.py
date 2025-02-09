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

    task_rdd_17 = simulate_task(base_rdd, '17', 21)
    task_rdd_13 = simulate_task(base_rdd, '13', 127)
    task_rdd_9 = simulate_task(base_rdd, '9', 6)
    task_rdd_7 = simulate_task(base_rdd, '7', 193)
    task_rdd_4 = simulate_task(base_rdd, '4', 12)
    task_rdd_12 = simulate_task(base_rdd, '12', 2)
    task_rdd_11 = simulate_task(base_rdd, '11', 9)
    task_rdd_10 = simulate_task(base_rdd, '10', 16)
    task_rdd_21 = simulate_task(base_rdd, '21', 6)
    task_rdd_5 = simulate_task(base_rdd, '5', 2)
    task_rdd_2 = simulate_task(base_rdd, '2', 78)
    task_rdd_19 = simulate_task(base_rdd, '19', 6)
    task_rdd_18 = simulate_task(base_rdd, '18', 84)
    task_rdd_6 = simulate_task(base_rdd, '6', 61)
    task_rdd_3 = simulate_task(base_rdd, '3', 3)
    task_rdd_1 = simulate_task(base_rdd, '1', 7)
    task_rdd_53 = simulate_task(base_rdd, '53', 3)
    task_rdd_132 = simulate_task(base_rdd, '132', 4)
    task_rdd_75 = simulate_task(base_rdd, '75', 4)
    task_rdd_36 = simulate_task(base_rdd, '36', 4)
    task_rdd_48 = simulate_task(base_rdd, '48', 12)
    task_rdd_45 = simulate_task(base_rdd, '45', 147)
    task_rdd_55 = simulate_task(base_rdd, '55', 5)
    task_rdd_44 = simulate_task(base_rdd, '44', 93)
    task_rdd_35 = simulate_task(base_rdd, '35', 7)
    task_rdd_24 = simulate_task(base_rdd, '24', 12)
    task_rdd_30 = simulate_task(base_rdd, '30', 28)
    task_rdd_25 = simulate_task(base_rdd, '25', 10)
    task_rdd_67 = simulate_task(base_rdd, '67', 107)
    task_rdd_65 = simulate_task(base_rdd, '65', 4)

    union_rdd_8 = spark.sparkContext.union([task_rdd_1, task_rdd_10, task_rdd_13])
    task_rdd_8 = simulate_task(union_rdd_8, '8', 12)

    union_rdd_22 = spark.sparkContext.union([task_rdd_21])
    task_rdd_22 = simulate_task(union_rdd_22, '22', 12)

    union_rdd_20 = spark.sparkContext.union([task_rdd_3, task_rdd_17, task_rdd_19])
    task_rdd_20 = simulate_task(union_rdd_20, '20', 30)

    union_rdd_70 = spark.sparkContext.union([task_rdd_53])
    task_rdd_70 = simulate_task(union_rdd_70, '70', 101)

    union_rdd_120 = spark.sparkContext.union([task_rdd_132])
    task_rdd_120 = simulate_task(union_rdd_120, '120', 4)

    union_rdd_31 = spark.sparkContext.union([task_rdd_30])
    task_rdd_31 = simulate_task(union_rdd_31, '31', 15)

    union_rdd_37 = spark.sparkContext.union([task_rdd_36])
    task_rdd_37 = simulate_task(union_rdd_37, '37', 88)

    union_rdd_49 = spark.sparkContext.union([task_rdd_45, task_rdd_48])
    task_rdd_49 = simulate_task(union_rdd_49, '49', 194)

    union_rdd_32 = spark.sparkContext.union([task_rdd_24])
    task_rdd_32 = simulate_task(union_rdd_32, '32', 15)

    union_rdd_26 = spark.sparkContext.union([task_rdd_25])
    task_rdd_26 = simulate_task(union_rdd_26, '26', 3)

    union_rdd_68 = spark.sparkContext.union([task_rdd_67])
    task_rdd_68 = simulate_task(union_rdd_68, '68', 21)

    union_rdd_62 = spark.sparkContext.union([task_rdd_8])
    task_rdd_62 = simulate_task(union_rdd_62, '62', 288)

    union_rdd_23 = spark.sparkContext.union([task_rdd_5, task_rdd_22])
    task_rdd_23 = simulate_task(union_rdd_23, '23', 272)

    union_rdd_43 = spark.sparkContext.union([task_rdd_1, task_rdd_2, task_rdd_3, task_rdd_6, task_rdd_18, task_rdd_19, task_rdd_20, task_rdd_2])
    task_rdd_43 = simulate_task(union_rdd_43, '43', 2)

    union_rdd_66 = spark.sparkContext.union([task_rdd_120])
    task_rdd_66 = simulate_task(union_rdd_66, '66', 4)

    union_rdd_54 = spark.sparkContext.union([task_rdd_31, task_rdd_75])
    task_rdd_54 = simulate_task(union_rdd_54, '54', 5)

    union_rdd_51 = spark.sparkContext.union([task_rdd_44, task_rdd_48, task_rdd_49])
    task_rdd_51 = simulate_task(union_rdd_51, '51', 40)

    union_rdd_33 = spark.sparkContext.union([task_rdd_32])
    task_rdd_33 = simulate_task(union_rdd_33, '33', 638)

    union_rdd_69 = spark.sparkContext.union([task_rdd_65, task_rdd_68])
    task_rdd_69 = simulate_task(union_rdd_69, '69', 40)

    union_rdd_63 = spark.sparkContext.union([task_rdd_12, task_rdd_62])
    task_rdd_63 = simulate_task(union_rdd_63, '63', 22)

    union_rdd_71 = spark.sparkContext.union([task_rdd_66, task_rdd_70])
    task_rdd_71 = simulate_task(union_rdd_71, '71', 5)

    union_rdd_56 = spark.sparkContext.union([task_rdd_54])
    task_rdd_56 = simulate_task(union_rdd_56, '56', 72)

    union_rdd_34 = spark.sparkContext.union([task_rdd_33])
    task_rdd_34 = simulate_task(union_rdd_34, '34', 6)

    union_rdd_58 = spark.sparkContext.union([task_rdd_71])
    task_rdd_58 = simulate_task(union_rdd_58, '58', 1)

    union_rdd_38 = spark.sparkContext.union([task_rdd_34, task_rdd_35])
    task_rdd_38 = simulate_task(union_rdd_38, '38', 19)

    union_rdd_59 = spark.sparkContext.union([task_rdd_51, task_rdd_56, task_rdd_58])
    task_rdd_59 = simulate_task(union_rdd_59, '59', 10)

    union_rdd_61 = spark.sparkContext.union([task_rdd_59])
    task_rdd_61 = simulate_task(union_rdd_61, '61', 1)

    union_rdd_50 = spark.sparkContext.union([task_rdd_61])
    task_rdd_50 = simulate_task(union_rdd_50, '50', 42)

    union_rdd_46 = spark.sparkContext.union([task_rdd_48, task_rdd_50])
    task_rdd_46 = simulate_task(union_rdd_46, '46', 5)

    union_rdd_47 = spark.sparkContext.union([task_rdd_31, task_rdd_38, task_rdd_46])
    task_rdd_47 = simulate_task(union_rdd_47, '47', 1)

    union_rdd_27 = spark.sparkContext.union([task_rdd_25, task_rdd_26, task_rdd_47])
    task_rdd_27 = simulate_task(union_rdd_27, '27', 154)

    union_rdd_28 = spark.sparkContext.union([task_rdd_5, task_rdd_27])
    task_rdd_28 = simulate_task(union_rdd_28, '28', 95)

    union_rdd_29 = spark.sparkContext.union([task_rdd_21, task_rdd_24, task_rdd_27, task_rdd_28])
    task_rdd_29 = simulate_task(union_rdd_29, '29', 2)

    union_rdd_14 = spark.sparkContext.union([task_rdd_5, task_rdd_13, task_rdd_29])
    task_rdd_14 = simulate_task(union_rdd_14, '14', 38)

    union_rdd_15 = spark.sparkContext.union([task_rdd_3, task_rdd_6, task_rdd_9, task_rdd_11, task_rdd_14])
    task_rdd_15 = simulate_task(union_rdd_15, '15', 13)

    union_rdd_16 = spark.sparkContext.union([task_rdd_2, task_rdd_4, task_rdd_6, task_rdd_9, task_rdd_15])
    task_rdd_16 = simulate_task(union_rdd_16, '16', 2)

    combined_rdd = spark.sparkContext.union([task_rdd_6, task_rdd_16])
    task_rdd = simulate_task(combined_rdd, 'R17_6_16', 15)
    task_rdd.collect()  # Trigger RDD computation
    spark.stop()
