def get_command_template():
    max_execs = 25    # max number of executors for any job, avoids issue with Spark dynamic allocation causing Spark to hang on Kubernetes

    # Define the paths and URLs
    SPARK_SUBMIT_PATH = "/home/cc/experiment-driver/spark/bin/spark-submit"
    K8S_CLUSTER_URL = "k8s://https://127.0.0.1:6443"
    SPARK_CONTAINER_IMAGE = ## PASTE THE TAG OF A SPARK IMAGE COMPILED BY /prototype/modified-spark/ HERE
    SPARK_PY_CONTAINER_IMAGE = ## PASTE THE TAG OF A SPARK-PY IMAGE COMPILED BY /prototype/modified-spark/ HERE

    # Define the Spark submit command template for SparkTC example
    SPARKTC_COMMAND_TEMPLATE = [
        SPARK_SUBMIT_PATH,
        "--master", K8S_CLUSTER_URL,
        "--deploy-mode", "cluster",
        "--name", "spark-pr",
        "--class", "org.apache.spark.examples.SparkTC",
        "--conf", f"spark.kubernetes.container.image={SPARK_CONTAINER_IMAGE}",
        "--conf", "spark.kubernetes.container.image.pullPolicy=IfNotPresent",
        "--conf", "spark.kubernetes.authenticate.driver.serviceAccountName=spark",
        "--conf", "spark.executor.cores=4",
        "--conf", "spark.executor.memory=7g",
        "--conf", "spark.dynamicAllocation.enabled=True",
        "--conf", "spark.dynamicAllocation.shuffleTracking.enabled=True",
        "--conf", f"spark.dynamicAllocation.maxExecutors={max_execs}",
        "--conf", "spark.kubernetes.namespace=spark-ns",
        "local:///opt/spark/examples/jars/spark-examples_2.12-3.5.3.jar",
    ]
    # Define the Spark submit command template for TPCH queries
    TPCH_BASE_COMMAND_TEMPLATE = [
        SPARK_SUBMIT_PATH,
        "--master", K8S_CLUSTER_URL,
        "--deploy-mode", "cluster",
        "--class", "main.scala.TpchQuery",
        "--name", "tpch",
        "--conf", f"spark.kubernetes.container.image={SPARK_CONTAINER_IMAGE}",
        "--conf", "spark.kubernetes.container.image.pullPolicy=IfNotPresent",
        "--conf", "spark.kubernetes.authenticate.driver.serviceAccountName=spark",
        "--conf", "spark.executor.cores=4",
        "--conf", "spark.executor.memory=7g",
        "--conf", "spark.dynamicAllocation.enabled=True",
        "--conf", "spark.dynamicAllocation.shuffleTracking.enabled=True",
        "--conf", f"spark.dynamicAllocation.maxExecutors={max_execs}",
        "--conf", "spark.kubernetes.namespace=spark-ns",
    ]
    # Define the Spark submit command template for Alibaba jobs
    ALIBABA_BASE_COMMAND_TEMPLATE = [
        SPARK_SUBMIT_PATH,
        "--master", K8S_CLUSTER_URL,
        "--deploy-mode", "cluster",
        "--class", "org.apache.spark.examples.SparkPi",
        "--name", "alibaba",
        "--conf", f"spark.kubernetes.container.image={SPARK_PY_CONTAINER_IMAGE}",
        "--conf", "spark.kubernetes.container.image.pullPolicy=IfNotPresent",
        "--conf", "spark.kubernetes.authenticate.driver.serviceAccountName=spark",
        "--conf", "spark.executor.cores=4",
        "--conf", "spark.executor.memory=7g",
        "--conf", "spark.dynamicAllocation.enabled=True",
        "--conf", "spark.dynamicAllocation.shuffleTracking.enabled=True",
        "--conf", f"spark.dynamicAllocation.maxExecutors={max_execs}",
        "--conf", "spark.kubernetes.namespace=spark-ns",
    ]

    # Define the TPCH 2G command template
    TPCH_2G_COMMAND_TEMPLATE = TPCH_BASE_COMMAND_TEMPLATE.copy()
    TPCH_2G_COMMAND_TEMPLATE.append("local:///opt/spark/examples/jars/2g-tpc-h-queries_2.12-1.0.jar")

    # Define the TPCH 10G command template
    TPCH_10G_COMMAND_TEMPLATE = TPCH_BASE_COMMAND_TEMPLATE.copy()
    TPCH_10G_COMMAND_TEMPLATE.append("local:///opt/spark/examples/jars/10g-tpc-h-queries_2.12-1.0.jar")

    # Define the TPCH 50G command template
    TPCH_50G_COMMAND_TEMPLATE = TPCH_BASE_COMMAND_TEMPLATE.copy()
    TPCH_50G_COMMAND_TEMPLATE.append("local:///opt/spark/examples/jars/50g-tpc-h-queries_2.12-1.0.jar")

    COMMAND_TEMPLATES = {
        "sparktc": SPARKTC_COMMAND_TEMPLATE,
        "tpch2g": TPCH_2G_COMMAND_TEMPLATE,
        "tpch10g": TPCH_10G_COMMAND_TEMPLATE,
        "tpch50g": TPCH_50G_COMMAND_TEMPLATE,
    }

    for i in range(1, 101):
        COMMAND_TEMPLATES[f"alibaba{i}"] = ALIBABA_BASE_COMMAND_TEMPLATE.copy()
        COMMAND_TEMPLATES[f"alibaba{i}"].append(f"local:///opt/spark/examples/job{i}.py")

    return COMMAND_TEMPLATES
