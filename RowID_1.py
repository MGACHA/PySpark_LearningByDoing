#!/usr/bin/env python
# coding: utf-8

# ## RowID_1
# 
# New notebook

# ## Row ID Generation in PySpark: 
# ## `monotonically_increasing_id` vs `zipWithIndex`
# 
# This repository demonstrates the two primary ways to generate 
# 
# unique row identifiers in Apache Spark when working 
# 
# with distributed datasets. 
# 

# ### Method 1: `monotonically_increasing_id()`
# * **What it does:** Generates unique, 64-bit integers. It is **extremely fast** 
# 
# because each data partition assigns IDs independently without talking to other nodes.
# * **The Catch:** The numbers are NOT consecutive. There will be massive gaps between partitions.
# * **Best for:** Database primary keys, heavy performance-critical joins, or multi-terabyte datasets.

# In[5]:


from pyspark.sql import functions as F

# Sample DataFrame split across 2 partitions
data = [("Kevin",), ("Bob",), ("Stuart",), ("Otto",), ("Dave",)]
df = spark.createDataFrame(data, ["name"]).repartition(2)

# Applying the fast ID function
df_mono = df.withColumn("row_id", F.monotonically_increasing_id())
df_mono.show()


# ### Method 2: df.rdd.zipWithIndex()
# * **What it does:** Generates perfectly consecutive numbers (0, 1, 2, 3...) with zero gaps.
# 
# * **The Catch:** It requires Spark to run an extra background job to count the rows 
# 
# in each partition first, causing a significant performance overhead.
# 
# * **Best for:** Machine Learning index mapping, ML features, 
# 
# or frontend pagination where sequential numbers are strictly required.

# In[6]:


# Sample DataFrame split across 2 partitions
data = [("Kevin",), ("Bobb",), ("Stuart",), ("Otto",), ("Dave",)]
df = spark.createDataFrame(data, ["name"]).repartition(2)

# To use zipWithIndex, we must drop down to the RDD layer, 
# apply the index, and reconstruct the DataFrame.
rdd_with_index = df.rdd.zipWithIndex()

# Re-mapping the structure from ((Name), Index) to (Name, Index)
mapped_rdd = rdd_with_index.map(lambda row: (row[0][0], row[1]))

# Convert back to DataFrame
df_zipped = spark.createDataFrame(mapped_rdd, ["name", "row_id"])
df_zipped.show()

