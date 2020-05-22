// Databricks notebook source
// MAGIC %md
// MAGIC # Transforming Data
// MAGIC 
// MAGIC ## In this lesson you:
// MAGIC * Learn to use basic aggregations.
// MAGIC * Column transformations
// MAGIC * Learn how to find distinct and duplicate values

// COMMAND ----------

// MAGIC %md
// MAGIC ### Let us get the test data first of all
// MAGIC Mount the training data set for Databricks S3 bucket by executing the following cell.

// COMMAND ----------

if (!dbutils.fs.mounts.map(mnt => mnt.mountPoint).contains("/mnt/training"))
  dbutils.fs.mount(
    source = "",
    mountPoint = "/mnt/training")    

println("Data from s3 is ready to use")

// COMMAND ----------

// MAGIC %md
// MAGIC ### Our usual practice of reading the data now

// COMMAND ----------

val peopleDF = spark.read.parquet("/mnt/training/dataframes/people-10m.parquet").cache()

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### Basic Transformations
// MAGIC 
// MAGIC Using <a href="https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$" target="_blank">built-in Spark functions</a>, you can aggregate data in various ways. 
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> By default, you get a floating point value.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ###Functions on dataframes
// MAGIC 
// MAGIC There are some functions that you can invoke directly on dataframes.  You can find a more complete list on the official docs.  We've seen the use of .show() already, but functions like .printSchema(), .count() and .head(5) are very useful to get oriented with your data.

// COMMAND ----------

peopleDF.printSchema

// COMMAND ----------

peopleDF.count()

// COMMAND ----------

peopleDF.head(5)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Databricks has a built in function "display()" that can also help you visualize your dataframe.

// COMMAND ----------

display(peopleDF)



// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Importing Functions
// MAGIC Some functions operate on columns.  For these you need to use the select on your dataframe to get access to the columns.
// MAGIC Reference the Spark docs for more details and functions.
// MAGIC https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.package
// MAGIC 
// MAGIC Note the import function in the next cell.
// MAGIC 
// MAGIC import org.apache.spark.sql.functions.avg
// MAGIC 
// MAGIC This is a common place for errors.  Though spark has some functions out of the box, most of the transformation functions need to be imported.  It's common to see the import statement written with the following Scala implicits syntax:
// MAGIC import org.apache.spark.sql.functions._

// COMMAND ----------

import org.apache.spark.sql.functions.avg
val avgSalaryDF = peopleDF.select(avg($"salary") as "averageSalary")



// COMMAND ----------

avgSalaryDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC Convert that value to an integer using the `round()` function. See
// MAGIC <a href="https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$" class="text-info">the documentation for <tt>round()</tt></a>
// MAGIC for more details.

// COMMAND ----------

// IMPORT FUNCTIONS HERE
import org.apache.spark.sql.functions._

val roundedAvgSalaryDF = avgSalaryDF.select(round($"averageSalary") as "averageSalary")

roundedAvgSalaryDF.show()

// COMMAND ----------

//SOME Tests !

lazy val results = roundedAvgSalaryDF.collect()

assert(Row(72633.0) == results(0), "The average salary is not correct")

println("Congratulations, the average salary computation is correct!")


// COMMAND ----------

// MAGIC %md
// MAGIC In addition to the rounded average salary, what are the minimum and maximum salaries?

// COMMAND ----------

val salaryDF = peopleDF.select(round(avg($"salary")) as "averageSalary", min($"salary") as "min", max($"salary") as "max")

salaryDF.show()

// COMMAND ----------

lazy val results = salaryDF.collect()

assert(Row(72633.0,-26884,180841) == results(0), "The average salary is not correct")

println("Congratulations, the average, minimum and maximum salary computations are correct!")


// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##GroupBy
// MAGIC Notice in the spark documentation that groupby does not return a dataset, but a RelationalGroupedDataset.  This means that you cannot chain dataset operations on it.  
// MAGIC 
// MAGIC https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset
// MAGIC 
// MAGIC If you look up RelationalGroupedDataset in the Spark docs you will see that you can use the agg function (along with some other functions) which will return a DataFrame.
// MAGIC 
// MAGIC https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.RelationalGroupedDataset
// MAGIC 
// MAGIC 
// MAGIC 
// MAGIC In the following example we calculate the average salary by gender.

// COMMAND ----------

val salaryByGenderDF = peopleDF.groupBy($"gender").agg(avg($"salary"))

salaryByGenderDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Now it's your turn, calculate the rounded average salary by firstname, and order by the average salary in decending order.

// COMMAND ----------

val avgSalaryByName = peopleDF.groupBy($"firstName").agg(round(avg($"salary")) as "averageSalary").orderBy($"averageSalary".desc)

avgSalaryByName.show()

// COMMAND ----------

lazy val results = avgSalaryByName.collect()

assert(Row("Sherry",74424.0) == results(0), "The average salary by name is not correct")
assert(Row("Remona",74312.0) == results(1), "The average salary by name is not correct")
assert(Row("Dessie",74309.0) == results(2), "The average salary by name is not correct")


println("Congratulations, the average salary by name calculation is correct!")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Now for a little more challenge and some practice with dates. Calculate the rounded average salary by birthyear and order by birthyear in asending order.

// COMMAND ----------

val avgSalaryByBirthYear = peopleDF.groupBy(year($"birthDate") as "birthYear").agg(round(avg($"salary"))).orderBy($"birthYear".asc)

avgSalaryByBirthYear.show()

// COMMAND ----------

lazy val results = avgSalaryByBirthYear.collect()

assert(Row(1951,72704.0) == results(0), "The average salary by birth year is not correct")
assert(Row(1952,72614.0) == results(1), "The average salary by birth year is not correct")
assert(Row(1953,72659.0) == results(2), "The average salary by birth year is not correct")

println("Congratulations, the average salary by birth year computations are correct!")


// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ###Adding columns
// MAGIC The .withColumnRenamed() function that operates on a dataframe is helpful to create new dataframes with additional columns.
// MAGIC 
// MAGIC Here is an example of that analyzes the average salary based on the length of a person's last name.

// COMMAND ----------

val lastNameLengthAvgSalaryDF = peopleDF
    .withColumn("lastNameLength", length($"lastName"))
    .groupBy($"lastNameLength")
    .agg(avg($"salary"))
    .orderBy($"lastNameLength")

// COMMAND ----------

lastNameLengthAvgSalaryDF.show()

// COMMAND ----------

// MAGIC %md Create a dataframe with the data from people df plus three new columns: birth year, birth month (1 to 12), and birth day (1 to 31).

// COMMAND ----------

// FILL IN
val newColumnsDF = peopleDF
    .withColumn("birthYear", year($"birthDate"))
    .withColumn("birthMonth", month($"birthDate"))
    .withColumn("birtDay", dayofmonth($"birthDate"))
newColumnsDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##Window
// MAGIC A window function calculates a return value for every input row of a table based on a group of rows, called the Frame. Every input row can have a unique frame associated with it. This characteristic of window functions makes them more powerful than other functions.
// MAGIC 
// MAGIC Spark SQL supports three kinds of window functions: ranking functions, analytic functions, and aggregate functions.
// MAGIC More information can be found: https://databricks.com/blog/2015/07/15/introducing-window-functions-in-spark-sql.html
// MAGIC 
// MAGIC In the following example, we will take a look to the function rank and dense_rank to calculate the highest and second highest salaries by gender.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ###Raking values
// MAGIC The .rank() function computes the rank of a value in a group of values. The result is one plus the number of rows preceding or equal to the current row in the ordering of the partition. The values will produce gaps in the sequence. (We will come back to this point at the end of this example)
// MAGIC 
// MAGIC Let's rank the employees considering their salaries.

// COMMAND ----------

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions.WindowSpec

val windowSpec = Window.partitionBy("gender").orderBy($"salary".desc)

val peopleSalaryRankDF = peopleDF
    .withColumn("rank", rank().over(windowSpec))

peopleSalaryRankDF.show()

// COMMAND ----------

// MAGIC %md 
// MAGIC Now that we have a rank of salaries by gender finding the highest and second highest salaries by gender is a piece of cake!

// COMMAND ----------

peopleSalaryRankDF.select("*").where(col("rank").leq(2)).show();

// COMMAND ----------

// MAGIC %md 
// MAGIC #####Note:
// MAGIC 
// MAGIC As we have used .rank() function, if there were 2 persons with the same salary they would have shared the same rank number. If for example they shares the highest salary value both would be rank **1**. However the following rank value to be assigned would not have been **2** but **3**. This is way it is mentiones that .rank() produce gaps in the sequence. Unlike the function rank, .dense_rank() will not produce gaps in the ranking sequence.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Explore the Data
// MAGIC 
// MAGIC Let's explore the peopleDF data by creating a table with only the columns that are used to identify names.

// COMMAND ----------

val namesDF = peopleDF
    .drop("id")
    .drop("ssn")
    .drop("birthDate")
    .drop("salary")
    .drop("gender")
    .cache()

namesDF.count()

// COMMAND ----------

// MAGIC %md Why there are 10,000,000 names? Does everyone have a different name?

// COMMAND ----------

// ADD YOUR ANALYSIS HERE
// dataframes do not need to have unique rows... this is a key difference between a relational table and a dataframe.  


// COMMAND ----------

// MAGIC %md
// MAGIC ### Explore on your own
// MAGIC 
// MAGIC That concludes the basic transformations introduction.  Feel free to explore the data in peopleDF further.  What other insights can you get from this data?
// MAGIC 
// MAGIC (Also, take a look at the jobs, stages, tasks and DAGs produced by the transformations.)
