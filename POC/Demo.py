# Databricks notebook source
americans = spark.createDataFrame(
[("bob", 42), ("lisa", 59)], ["first_name", "age"]
)
colombians = spark.createDataFrame(
[("maria", 20), ("camilo", 31)], ["first_name", "age"]
)
res = americans.union(colombians)
display(res)

# COMMAND ----------

details = [(1, 'Krishna', 'IT', 'male')]
column = ['id', 'name', 'department', 'gender']
details1 = [(1, 'Krishna', 'IT', 10000)]
column = ['id', 'name', 'department', 'salary']
df1 = spark.createDataFrame(details, column)
df2 = spark.createDataFrame(details1, column)
display(df1)
display(df2)


# COMMAND ----------

display(df1.union(df2))


# COMMAND ----------

df1.unionByName(df2, allowMissingColumns=True).show()


# COMMAND ----------

# union
data_frame1 = spark.createDataFrame(
[("Nitya", 82.98), ("Abhishek", 80.31)],
["Student Name", "Overall Percentage"]
)
# Creating another dataframe
data_frame2 = spark.createDataFrame(
[("Sandeep", 91.123), ("Rakesh", 90.51)],
["Student Name", "Overall Percentage"]
)
# union()
UnionEXP = data_frame1.union(data_frame2)
UnionEXP.show()


# COMMAND ----------

data_frame1 = spark.createDataFrame(
[("Nitya", 82.98), ("Abhishek", 80.31)],
["Student Name", "Overall Percentage"]
)
# Creating another data frame
data_frame2 = spark.createDataFrame(
[(91.123, "Naveen"), (90.51, "Sandeep"), (87.67, "Rakesh")],
["Overall Percentage", "Student Name"]
)
# Union both the dataframes using unionByName() method
byName = data_frame1.unionByName(data_frame2)
byName.show()


# COMMAND ----------

data_frame1 = spark.createDataFrame(
[("Bhuwanesh", 82.98, "Computer Science"), ("Harshit", 80.31, "Information Technology")],
["Student Name", "Overall Percentage", "Department"]
)
# Creating another dataframe
data_frame2 = spark.createDataFrame( [("Naveen", 91.123), ("Piyush", 90.51)], ["Student Name", "Overall Percentage"] )
# Union both the dataframes using unionByName() method
column_name_morein1df = data_frame1.unionByName(data_frame2, allowMissingColumns=True)
column_name_morein1df.show()


# COMMAND ----------

from pyspark.sql.window import Window
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("pyspark_window").getOrCreate()
sampleData = (("Nitya", 28, "Sales", 3000),
("Abhishek", 33, "Sales", 4600),
("Sandeep", 40, "Sales", 4100),
("Rakesh", 25, "Finance", 3000),
("Ram", 28, "Sales", 3000),
("Srishti", 46, "Management", 3300),
("Arbind", 26, "Finance", 3900),
("Hitesh", 30, "Marketing", 3000),
("Kailash", 29, "Marketing", 2000),
("Sushma", 39, "Sales", 4100)
)
# column names
columns = ["Employee_Name", "Age",
"Department", "Salary"]
# creating the dataframe df
df = spark.createDataFrame(data=sampleData,schema=columns)
windowPartition = Window.partitionBy("Department").orderBy("Age")
df.printSchema()
df.show()
display(df)

# COMMAND ----------

from pyspark.sql.functions import cume_dist
df.withColumn("cume_dist", cume_dist().over(windowPartition)).display()

# COMMAND ----------

from pyspark.sql.functions import lag
df.withColumn("Lag", lag("Salary", 2).over(windowPartition)) \
.display()

# COMMAND ----------

from pyspark.sql.functions import lead
df.withColumn("Lead", lead("salary", 2).over(windowPartition)) \
.display()

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("pyspark_window").getOrCreate()
sampleData = ((101, "Ram", "Biology", 80),
(103, "Sita", "Social Science", 78),
(104, "Lakshman", "Sanskrit", 58),
(102, "Kunal", "Phisycs", 89),
(101, "Ram", "Biology", 80),
(106, "Srishti", "Maths", 70),
(108, "Sandeep", "Physics", 75),
(107, "Hitesh", "Maths", 88),
(109, "Kailash", "Maths", 90),
(105, "Abhishek", "Social Science", 84)
)
columns = ["Roll_No", "Student_Name", "Subject", "Marks"]
df2 = spark.createDataFrame(data=sampleData,
schema=columns)
windowPartition = Window.partitionBy("Subject").orderBy("Marks")
df2.printSchema()
df2.display()

# COMMAND ----------

from pyspark.sql.functions import row_number
df2.withColumn("row_number", row_number().over(windowPartition)).display()

# COMMAND ----------

from pyspark.sql.functions import rank
df2.withColumn("rank", rank().over(windowPartition)) \
.display()


# COMMAND ----------

from pyspark.sql.functions import percent_rank
df2.withColumn("percent_rank",percent_rank().over(windowPartition)).display()


# COMMAND ----------

from pyspark.sql.functions import dense_rank
df2.withColumn("dense_rank",dense_rank().over(windowPartition)).display()

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("pyspark_window").getOrCreate()
sampleData = (("Ram", "Sales", 3000),
("Meena", "Sales", 4600),
("Abhishek", "Sales", 4100),
("Kunal", "Finance", 3000),
("Ram", "Sales", 3000),
("Srishti", "Management", 3300),
("Sandeep", "Finance", 3900),
("Hitesh", "Marketing", 3000),
("Kailash", "Marketing", 2000),
("Shyam", "Sales", 4100)
)
columns = ["Employee_Name", "Department", "Salary"]
df3 = spark.createDataFrame(data=sampleData,schema=columns)
df3.printSchema()
df3.display()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import col,avg,sum,min,max,row_number
windowPartitionAgg = Window.partitionBy("Department")
df3.withColumn("Avg",avg(col("salary")).over(windowPartitionAgg)).show()
# sum()
df3.withColumn("Sum",sum(col("salary")).over(windowPartitionAgg)).show()
#min()
df3.withColumn("Min",min(col("salary")).over(windowPartitionAgg)).show()
df3.withColumn("Max",max(col("salary")).over(windowPartitionAgg)).show()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DateType, BooleanType
spark = SparkSession.builder \
.master("local[*]") \
.appName("timestamp") \
.getOrCreate()
df = spark.createDataFrame([["1", "2019-07-01 12:01:19.000"], ["2", "2019-06-24 12:01:19.000"]], ["id", "input_timestamp"])
df.printSchema()
df.display()


# COMMAND ----------

df1 = df.withColumn("timestamptype", to_timestamp("input_timestamp"))
df1.display()


# COMMAND ----------

df2 = df1.select("id", "timestamptype").withColumnRenamed("timestamptype", "input_timestamp")
df2.display()

# COMMAND ----------

df3=df2.select(col("id"), col("input_timestamp").cast('string'))
df3.display()


# COMMAND ----------

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
spark = SparkSession \
.builder \
.appName("TopN") \
.master("local[*]") \
.getOrCreate()
sampledata = (("Nitya", "Sales", 3000), \
("Abhi", "Sales", 4600), \
("Rakesh", "Sales", 4100), \
("Sandeep", "finance", 3000), \
("Abhishek", "Sales", 3000), \
("Shyan", "finance", 3300), \
("Madan", "finance", 3900), \
("Jarin", "marketing", 3000), \
("kumar", "marketing", 2000))
columns = ["employee_name", "department", "Salary"]
df = spark.createDataFrame(data = sampledata, schema = columns)
df.display()


# COMMAND ----------

windowSpec = Window.partitionBy("department").orderBy("salary")
df1 = df.withColumn("row", row_number().over(windowSpec)) # applying row_number
df1.display()

# COMMAND ----------

df2 = df1.filter(col("row") <3)
df2.display()


# COMMAND ----------

# drop dublicate in emp dataframe using distinct or dropDublicates
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession \
.builder \
.appName("droppingDublicates") \
.master("local[*]") \
.getOrCreate()
sample_data = ([1, "ramesh", 1000], [2, "Krishna", 2000], [3, "Shri", 3000], [4, "Pradip",
4000],
[1, "ramesh", 1000], [2, "Krishna", 2000], [3, "Shri", 3000], [4, "Pradip",
4000])
columns = ["id", "name", "salary"]
df = spark.createDataFrame(data = sample_data, schema= columns)
df.display()

# COMMAND ----------

df1= df.distinct().show()

# COMMAND ----------

df3 = df.dropDuplicates().show()

# COMMAND ----------

df4 = df.select(["id", "name"]).distinct().show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, flatten
spark = SparkSession.builder.appName("explodeempdata").master("local[*]").getOrCreate()
arrayArrayData = [("Abhishek", [["Java", "scala", "perl"], ["spark","java"]]),
("Nitya", [["spark", "java", "c++"], ["spark", "java"]]),
("Sandeep", [["csharp", "vb"], ["spark", "python"]])]
df = spark.createDataFrame(data = arrayArrayData, schema = ['name', 'subjects'])
df.printSchema()
df.show()
df.select(df.name, explode(df.subjects)).show(truncate=False)
df.select(df.name, flatten(df.subjects)).show(truncate=False)

# COMMAND ----------

df.select(df.name, explode(df.subjects)).show(truncate=False)

# COMMAND ----------

df.select(df.name, flatten(df.subjects)).show(truncate=False)


# COMMAND ----------

data = [(1, "Abhishek", "10|30|40"),
(2, "Krishna", "50|40|70"),
(3, "rakesh", "20|70|90")]
df = spark.createDataFrame(data, schema=["id", "name", "marks"])
df.show()


# COMMAND ----------

from pyspark.sql.functions import split, col
df_s = df.withColumn("mark_details", split(col("marks"), "[|]")) \
.withColumn("maths", col("mark_details")[0]) \
.withColumn("physics", col("mark_details")[1]) \
.withColumn("chemistry", col("mark_details")[2])
display(df_s)

# COMMAND ----------

from pyspark.sql.functions import split, col
df_s = df.withColumn("mark_details", split(col("marks"), "[|]")) \
.withColumn("maths", col("mark_details")[0]) \
.withColumn("physics", col("mark_details")[1]) \
.withColumn("chemistry", col("mark_details")[2]).drop("mark_details", "marks")
display(df_s)

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("example").getOrCreate()

# Create a DataFrame
data = [("John", 25), ("Alice", 30), ("Bob", 22)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# Show the DataFrame
df.show()


# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.{ SparkSession }
# MAGIC
# MAGIC // Create a DataFrame in Scala
# MAGIC val spark = SparkSession.builder().appName("example").getOrCreate()
# MAGIC
# MAGIC val data = Seq(("John", 25), ("Alice", 30), ("Bob", 22))
# MAGIC val columns = Seq("Name", "Age")
# MAGIC val df = spark.createDataFrame(data).toDF(columns: _*)
# MAGIC
# MAGIC // Show the DataFrame
# MAGIC df.show()

# COMMAND ----------

# MAGIC %r
# MAGIC # Load SparkR library
# MAGIC library(SparkR)
# MAGIC
# MAGIC # Initialize Spark session
# MAGIC sparkR.session(appName = "example")
# MAGIC
# MAGIC # Create a DataFrame in R
# MAGIC data <- data.frame(Name = c("John", "Alice", "Bob"), Age = c(25, 30, 22))
# MAGIC df <- createDataFrame(data)
# MAGIC
# MAGIC # Show the DataFrame
# MAGIC head(df)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a DataFrame using SQL in Databricks
# MAGIC CREATE OR REPLACE TEMPORARY VIEW example_view AS
# MAGIC SELECT "John" as Name, 25 as Age
# MAGIC UNION
# MAGIC SELECT "Alice" as Name, 30 as Age
# MAGIC UNION
# MAGIC SELECT "Bob" as Name, 22 as Age;
# MAGIC
# MAGIC -- Show the DataFrame
# MAGIC SELECT * FROM example_view;
# MAGIC
