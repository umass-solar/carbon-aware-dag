#!/bin/bash

# This script is used to setup the namespace, service account, cluster role binding, and resource quota for the Spark cluster.

# Create namespace
kubectl create namespace spark-ns

# Create service account
kubectl create serviceaccount spark -n spark-ns

# Create cluster role binding
kubectl create clusterrolebinding spark-role --clusterrole=edit --serviceaccount=spark-ns:spark --namespace=spark-ns

# Apply resource quota
kubectl apply -f resource_quota_template.yaml -n spark-ns