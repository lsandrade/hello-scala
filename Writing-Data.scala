// Databricks notebook source
// MAGIC %md
// MAGIC # Writing data with Spark
// MAGIC 
// MAGIC 
// MAGIC ## Lesson
// MAGIC 
// MAGIC 
// MAGIC ## In this lesson you:
// MAGIC * Write data as csv or parquet
// MAGIC * See an example of Schema Evolution
// MAGIC * Learning a few databricks magic commands along the way

// COMMAND ----------

// MAGIC %md
// MAGIC #### First Download this file
// MAGIC 
// MAGIC 
// MAGIC Download the following file to your local machine: <a href="https://s3-us-west-2.amazonaws.com/databricks-corp-training/common/dataframes/state-income.csv">state-income.csv</a>

// COMMAND ----------

//Command to Create directory:
dbutils.fs.mkdirs("/tempdir/")

// COMMAND ----------

// Command to Write file:
dbutils.fs.put("/tempdir/foo.txt", "Hello world")

// COMMAND ----------

//Command to Read file:
dbutils.fs.head("/tempdir/foo.txt")

// COMMAND ----------

//Command to Remove file:
dbutils.fs.rm("/tempdir/foo.txt")

// COMMAND ----------

//Writing data activity:
//Create Dataframe:
case class Employee(firstName: String, lastName: String, email: String, salary: Int)

  val employee1 = new Employee("michael", "armbrust", "no-reply@berkeley.edu", 100000)
  val employee2 = new Employee("xiangrui", "meng", "no-reply@stanford.edu", 120000)
  val employee3 = new Employee("matei", null, "no-reply@waterloo.edu", 140000)

  val employees = Seq(employee1, employee2, employee3)
  val df = employees.toDF()

// COMMAND ----------

//Remove file if it exists
dbutils.fs.rm("/tmp/employees.csv", true)

// COMMAND ----------

// List all the files created
display(dbutils.fs.ls("/tmp/employees.csv"))

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Assignment 1
// MAGIC 
// MAGIC Write the employee information as a csv.

// COMMAND ----------

// FILL IN



// COMMAND ----------

// List all the files created
display(dbutils.fs.ls("/tmp/employees.csv"))



// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Assignment 2
// MAGIC 
// MAGIC Write the employee information as a parquet.

// COMMAND ----------

//Remove file if it exists
dbutils.fs.rm("/tmp/employees.parquet", true)

// COMMAND ----------

// FILL IN


// COMMAND ----------

display(dbutils.fs.ls("/tmp/employees.parquet"))

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Assignment 3
// MAGIC ### Schema Evolution
// MAGIC Assume that we have a customer with two attibutes which want to persist today.

// COMMAND ----------

case class Customer(id: Int, name: String)

val customerDF = Seq(Customer (1, "Ramesh"), Customer (2, "Suresh"), Customer (3, "Mahesh")).toDF 

customerDF.write.parquet("/tmp/customers.parquet")

// COMMAND ----------

// MAGIC %md
// MAGIC ### Now the business evolves and we have a new attribute as "regions"

// COMMAND ----------

val customersPath = "/tmp/customers.parquet"

case class Customer(id: Int, name: String, region:String)

val customerDF = Seq(Customer (4, "Seeta", "AP"), Customer (5, "Geeta", "EMEA"), Customer (6, "Sangeeta", "US")).toDF 

// Now append the above records in the previous customers parquet directory 
 //FILL IN (Check this link) - https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/DataFrameWriter.html#method.summary


// COMMAND ----------

// MAGIC %md
// MAGIC ###What do you think will happen if you read the customers.parquet file now ?

// COMMAND ----------

spark.read.parquet("/tmp/customers.parquet").show()

// COMMAND ----------

// MAGIC %md 
// MAGIC ## For more reference:
// MAGIC 
// MAGIC *  https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html.  
// MAGIC    - Notice that the API for reading is very similar to that of writing.  
// MAGIC   
// MAGIC   Here is the official spark documentation on the write API: <br>
// MAGIC   <a href="https://docs.databricks.com/spark/latest/dataframes-datasets/introduction-to-dataframes-scala.html" target="_blank">Introduction to DataFrames using Scala</a><br>
// MAGIC   <a href="https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameWriter" target="_blank">Reference to Spark Documentation on how to write data</a>
