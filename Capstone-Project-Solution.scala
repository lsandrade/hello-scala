// Databricks notebook source
// MAGIC %md
// MAGIC # Project: Exploratory Data Analysis
// MAGIC Perform exploratory data analysis (EDA) to gain insights from a data lake.
// MAGIC 
// MAGIC ## Instructions
// MAGIC 
// MAGIC In `dbfs:/mnt/training/crime-data-2016`, there are a number of Parquet files containing 2016 crime data from seven United States cities:
// MAGIC 
// MAGIC * New York
// MAGIC * Los Angeles
// MAGIC * Chicago
// MAGIC * Philadelphia
// MAGIC * Dallas
// MAGIC * Boston
// MAGIC 
// MAGIC 
// MAGIC The data is cleaned up a little, but has not been normalized. Each city reports crime data slightly differently, so
// MAGIC examine the data for each city to determine how to query it properly.
// MAGIC 
// MAGIC Your job is to use some of this data to gain insights about certain kinds of crimes.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Getting Started
// MAGIC 
// MAGIC Run the following cell to configure our "classroom."

// COMMAND ----------

if (!dbutils.fs.mounts.map(mnt => mnt.mountPoint).contains("/mnt/training"))
  dbutils.fs.mount(
    source = "s3a://AKIAJBRYNXGHORDHZB4A:a0BzE1bSegfydr3%2FGE3LSPM6uIV5A4hOUfpH8aFF@databricks-corp-training/common",
    mountPoint = "/mnt/training")    

println("Data from s3 is ready to use")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Step 1
// MAGIC 
// MAGIC Start by creating DataFrames for Los Angeles, Philadelphia, and Dallas data.
// MAGIC 
// MAGIC Use `spark.read.parquet` to create named DataFrames for the files you choose. 
// MAGIC 
// MAGIC To read in the parquet file, use `val crimeDataNewYorkDF = spark.read.parquet("/mnt/training/crime-data-2016/Crime-Data-New-York-2016.parquet")`
// MAGIC 
// MAGIC Use the following view names:
// MAGIC 
// MAGIC | City          | DataFrame Name            | Path to DBFS file
// MAGIC | ------------- | ------------------------- | -----------------
// MAGIC | Los Angeles   | `crimeDataLosAngelesDF`   | `dbfs:/mnt/training/crime-data-2016/Crime-Data-Los-Angeles-2016.parquet`
// MAGIC | Philadelphia  | `crimeDataPhiladelphiaDF` | `dbfs:/mnt/training/crime-data-2016/Crime-Data-Philadelphia-2016.parquet`
// MAGIC | Dallas        | `crimeDataDallasDF`       | `dbfs:/mnt/training/crime-data-2016/Crime-Data-Dallas-2016.parquet`

// COMMAND ----------

// MAGIC %md
// MAGIC #### Los Angeles

// COMMAND ----------

// TODO- Done
val crimeDataLosAngelesDF = spark.read.parquet("dbfs:/mnt/training/crime-data-2016/Crime-Data-Los-Angeles-2016.parquet")

// COMMAND ----------

// TEST - Run this cell to test your solution.

lazy val rowsLosAngeles  = crimeDataLosAngelesDF.count()
assert(rowsLosAngeles == 217945, "the crime data count is incorrect")

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Philadelphia

// COMMAND ----------

// TODO - DOne

val crimeDataPhiladelphiaDF = spark.read.parquet("dbfs:/mnt/training/crime-data-2016/Crime-Data-Philadelphia-2016.parquet")


// COMMAND ----------

// TEST - Run this cell to test your solution.

lazy val rowsPhiladelphia  = crimeDataPhiladelphiaDF.count()
assert(rowsPhiladelphia == 168664, "the crime data count is incorrect")

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Dallas

// COMMAND ----------

// TODO - Done


val crimeDataDallasDF = spark.read.parquet("dbfs:/mnt/training/crime-data-2016/Crime-Data-Dallas-2016.parquet")

// COMMAND ----------

// TEST - Run this cell to test your solution.

lazy val rowsDallas  = crimeDataDallasDF.count()
assert(99642 == rowsDallas, "the crime data count is incorrect")

println("Tests passed!")

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ## Step 2
// MAGIC 
// MAGIC For each table, examine the data to figure out how to extract _robbery_ statistics.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Each city uses different values to indicate robbery. Commonly used terminology is "larceny", "burglary" or "robbery."  These challenges are common in Data Lakes.  Restrict yourself to only robberies.
// MAGIC 
// MAGIC Explore the data for the three cities until you understand how each city records robbery information. If you don't want to worry about upper- or lower-case, 
// MAGIC remember to use the DataFrame `lower()` method to converts column values to lowercase.
// MAGIC 
// MAGIC Create a DataFrame containing only the robbery-related rows, as shown in the table below.
// MAGIC 
// MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** For each table, focus your efforts on the column listed below.
// MAGIC 
// MAGIC Focus on the following columns for each table:
// MAGIC 
// MAGIC | DataFrame Name            | Robbery DataFrame Name  | Column
// MAGIC | ------------------------- | ----------------------- | -------------------------------
// MAGIC | `crimeDataLosAngelesDF`   | `robberyLosAngelesDF`   | `crimeCodeDescription`
// MAGIC | `crimeDataPhiladelphiaDF` | `robberyPhiladelphiaDF` | `ucr_general_description`
// MAGIC | `crimeDataDallasDF`       | `robberyDallasDF`       | `typeOfIncident`

// COMMAND ----------

// MAGIC %md
// MAGIC #### Los Angeles

// COMMAND ----------

// TODO - Done
import org.apache.spark.sql.functions.lower
val robberyLosAngelesDF = crimeDataLosAngelesDF.filter(lower($"crimeCodeDescription") === "robbery")
println(robberyLosAngelesDF.count())


// COMMAND ----------

// TEST - Run this cell to test your solution.

lazy val totalLosAngeles  = robberyLosAngelesDF.count()
assert(9048 == totalLosAngeles, "the robbery count is incorrect")

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Philadelphia

// COMMAND ----------

// TODO - Done

val robberyPhiladelphiaDF = crimeDataPhiladelphiaDF.filter(lower($"ucr_general_description") === "robbery")
println(robberyPhiladelphiaDF.count())

// COMMAND ----------

// TEST - Run this cell to test your solution.

lazy val totalPhiladelphia  = robberyPhiladelphiaDF.count()
assert(6149 == totalPhiladelphia, "the robbery data is incorrect")

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Dallas

// COMMAND ----------

// TODO - Done

val robberyDallasDF = crimeDataDallasDF.filter(lower($"typeOfIncident").contains("robbery") && !lower($"typeOfIncident").contains("burglary"))

println(robberyDallasDF.count())

// COMMAND ----------

// TEST - Run this cell to test your solution.

lazy val totalDallas = robberyDallasDF.count()
assert(6824 == totalDallas, "the robbery count is incorrect")

println("Tests passed!")

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ## Step 3
// MAGIC 
// MAGIC Now that you have DataFrames of only the robberies in each city, create DataFrames for each city summarizing the number of robberies in each month.
// MAGIC 
// MAGIC Your DataFrames must contain two columns:
// MAGIC * `month`: The month number (e.g., 1 for January, 2 for February, etc.).
// MAGIC * `robberies`: The total number of robberies in the month.
// MAGIC 
// MAGIC Use the following DataFrame names and date columns:
// MAGIC 
// MAGIC 
// MAGIC | City          | DataFrame Name     | Date Column 
// MAGIC | ------------- | ------------- | -------------
// MAGIC | Los Angeles   | `robberiesByMonthLosAngelesDF` | `timeOccurred`
// MAGIC | Philadelphia  | `robberiesByMonthPhiladelphiaDF` | `dispatch_date_time`
// MAGIC | Dallas        | `robberiesByMonthDallasDF` | `startingDateTime`
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> For each city, figure out which column contains the date of the incident. Then, extract the month from that date.

// COMMAND ----------

// MAGIC %md
// MAGIC #### Los Angeles

// COMMAND ----------

// TODO - Done

import org.apache.spark.sql.functions.{month,count}
val robberiesByMonthLosAngelesDF = robberyLosAngelesDF.groupBy(month($"timeOccurred") as "month").count.toDF("month","robberies").orderBy("month")
robberiesByMonthLosAngelesDF.show

// COMMAND ----------

// TEST - Run this cell to test your solution.

lazy val la = robberiesByMonthLosAngelesDF.collect().toList
assert(List(Row(1,719), Row(2,675), Row(3,709), Row(4,713), Row(5,790), Row(6,698), Row(7,826), Row(8,765), Row(9,722), Row(10,814), Row(11,764), Row(12,853)) == la, "the robbery count is incorrect")

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Philadelphia

// COMMAND ----------

// TODO - Done

import org.apache.spark.sql.functions._

val robberiesByMonthPhiladelphiaDF = robberyPhiladelphiaDF.groupBy(month($"dispatch_date_time") as "month").count.toDF("month","robberies").orderBy("month")
robberiesByMonthPhiladelphiaDF.show

// COMMAND ----------

// TEST - Run this cell to test your solution.
// convert to list so that we get deep compare (Array would be a shallow compare)
lazy val philadelphia  = robberiesByMonthPhiladelphiaDF.collect().toList
assert(List(Row(1,520), Row(2,416), Row(3,432), Row(4,466), Row(5,533), Row(6,509), Row(7,537), Row(8,561), Row(9,514), Row(10,572), Row(11,545), Row(12,544)) == philadelphia, "the robberies by month data is incorrect")

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Dallas

// COMMAND ----------

// TODO - Done

import org.apache.spark.sql.functions._

val robberiesByMonthDallasDF = robberyDallasDF.groupBy(month($"startingDateTime") as "month").count.toDF("month","robberies").orderBy("month")
robberiesByMonthDallasDF.show

// COMMAND ----------

// TEST - Run this cell to test your solution.

lazy val dallas  = robberiesByMonthDallasDF.collect().toList
assert(List(Row(1, 743), Row(2, 435), Row(3,412), Row(4,594), Row(5,615), Row(6,495), Row(7,535), Row(8,627), Row(9,512), Row(10,603), Row(11,589), Row(12,664)) == dallas, "the robberies by month data is incorrect")

println("Tests passed!")

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC 
// MAGIC ## Step 4
// MAGIC 
// MAGIC Plot the robberies per month for each of the three cities, producing a plot similar to the following:
// MAGIC 
// MAGIC <img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/robberies-by-month.png" style="max-width: 700px; border: 1px solid #aaaaaa; border-radius: 10px 10px 10px 10px"/>
// MAGIC 
// MAGIC When you first run the cell, you'll get an HTML table as the result. To configure the plot:
// MAGIC 
// MAGIC 1. Click the graph button.
// MAGIC 2. If the plot doesn't look correct, click the **Plot Options** button.
// MAGIC 3. Configure the plot similar to the following example.
// MAGIC 
// MAGIC <img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/capstone-plot-1.png" style="width: 440px; margin: 10px; border: 1px solid #aaaaaa; border-radius: 10px 10px 10px 10px"/>
// MAGIC <img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/capstone-plot-2.png" style="width: 268px; margin: 10px; border: 1px solid #aaaaaa; border-radius: 10px 10px 10px 10px"/>
// MAGIC <img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/capstone-plot-3.png" style="width: 362px; margin: 10px; border: 1px solid #aaaaaa; border-radius: 10px 10px 10px 10px"/>

// COMMAND ----------

// MAGIC %md
// MAGIC #### Los Angeles

// COMMAND ----------

// TODO - Done
display(robberiesByMonthLosAngelesDF)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Philadelphia

// COMMAND ----------

// TODO - Done

display(robberiesByMonthPhiladelphiaDF)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Dallas

// COMMAND ----------

// TODO - Done

display(robberiesByMonthDallasDF)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ## Step 5
// MAGIC 
// MAGIC Create another DataFrame called `combinedRobberiesByMonthDF`, that combines all three robberies-per-month views into one.
// MAGIC In creating this view, add a new column called `city`, that identifies the city associated with each row.
// MAGIC The final view will have the following columns:
// MAGIC 
// MAGIC * `city`: The name of the city associated with the row. (Use the strings "Los Angeles", "Philadelphia", and "Dallas".)
// MAGIC * `month`: The month number associated with the row.
// MAGIC * `robbery`: The number of robbery in that month (for that city).
// MAGIC 
// MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** You may want to apply the `union()` method in this example to combine the three datasets.
// MAGIC 
// MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** It's easy to add new columns in DataFrames. For example, add a new column called `newColumn` to `originalDF` use `withColumn()` method as follows:
// MAGIC 
// MAGIC ```originalDF.withColumn("newColumn")``` 

// COMMAND ----------

// TODO - Done

val combinedRobberiesByMonthDF  = robberiesByMonthLosAngelesDF
  .withColumn("city", lit("Los Angeles"))
  .union(
    robberiesByMonthPhiladelphiaDF.withColumn("city", lit("Philadelphia"))
  )
  .union(
    robberiesByMonthDallasDF.withColumn("city", lit("Dallas"))
  )
  .select($"city", $"month", $"robberies")


// COMMAND ----------


// TEST - Run this cell to test your solution.

lazy val results = combinedRobberiesByMonthDF.collect().toSet
           
assert(Set(Row("Dallas",11,589), Row("Los Angeles",2,675), Row("Dallas",8,627), Row("Los Angeles",9,722), Row("Los Angeles",1,719), Row("Philadelphia",12,544), Row("Dallas",1,743), Row("Dallas",10,603), Row("Dallas",6,495), Row("Los Angeles",4,713), Row("Philadelphia",2,416), Row("Dallas",4,594), Row("Los Angeles",12,853), Row("Dallas",12,664), Row("Dallas",9,512), Row("Los Angeles",3,709), Row("Dallas",2,435), Row("Los Angeles",7,826), Row("Philadelphia",1,520), Row("Los Angeles",5,790), Row("Philadelphia",7,537), Row("Dallas",5,615), Row("Philadelphia",9,514), Row("Los Angeles",6,698), Row("Philadelphia",8,561), Row("Los Angeles",11,764), Row("Philadelphia",6,509), Row("Dallas",3,412), Row("Philadelphia",5,533), Row("Philadelphia",10,572), Row("Los Angeles",10,814), Row("Los Angeles",8,765), Row("Philadelphia",11,545), Row("Dallas",7,535), Row("Philadelphia",3,432), Row("Philadelphia",4,466)) == results, "the robberies by month data is incorrect")

println("Tests passed!")

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ## Step 6
// MAGIC 
// MAGIC Graph the contents of `combinedRobberiesByMonthDF`, producing a graph similar to the following. (The diagram below deliberately
// MAGIC uses different data.)
// MAGIC 
// MAGIC <img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/combined-homicides.png" style="width: 800px; border: 1px solid #aaaaaa; border-radius: 10px 10px 10px 10px"/>
// MAGIC 
// MAGIC Adjust the plot options to configure the plot properly, as shown below:
// MAGIC 
// MAGIC <img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/capstone-plot-4.png" style="width: 362px; margin: 10px; border: 1px solid #aaaaaa; border-radius: 10px 10px 10px 10px"/>
// MAGIC 
// MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Order your results by `month`, then `city`.

// COMMAND ----------

// TODO - Done

display(combinedRobberiesByMonthDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Step 7
// MAGIC 
// MAGIC While the above graph is interesting, it's flawed: it's comparing the raw numbers of robberies, not the per capita robbery rates.
// MAGIC 
// MAGIC The DataFrame (already created) called `cityDataDF`  contains, among other data, estimated 2016 population values for all United States cities
// MAGIC with populations of at least 100,000. (The data is from [Wikipedia](https://en.wikipedia.org/wiki/List_of_United_States_cities_by_population).)
// MAGIC 
// MAGIC * Use the population values in that table to normalize the robberies so they represent per-capita values (total robberies divided by population).
// MAGIC * Save your results in a DataFrame called `robberyRatesByCityDF`.
// MAGIC * The robbery rate value must be stored in a new column, `robberyRate`.
// MAGIC 
// MAGIC Next, graph the results, as above.

// COMMAND ----------

// TODO - Done

// rename "city" column to "cities" to avoid having two similarly named cols; also columns are case-blind
val cityDataDF = spark.read.parquet("dbfs:/mnt/training/City-Data.parquet").withColumnRenamed("city", "cities")

import org.apache.spark.sql.functions.format_number
val robberyRatesByCityDF = combinedRobberiesByMonthDF
  .join(cityDataDF, $"city" === $"cities")
  .withColumn("robberyRate", format_number($"robberies" / $"estPopulation2016", 6))
  .select($"city", $"month", $"robberyRate")

display(robberyRatesByCityDF)

// COMMAND ----------

// TEST - Run this cell to test your solution.
lazy val results = robberyRatesByCityDF.select($"city", $"month", $"robberyRate").orderBy($"city", $"month").collect.toList
lazy val expectedResults = List(
  Row("Dallas",  1, "0.000564"),
  Row("Dallas",  2, "0.000330"),
  Row("Dallas",  3, "0.000313"),
  Row("Dallas",  4, "0.000451"), 
  Row("Dallas",  5, "0.000467"),
  Row("Dallas",  6, "0.000376"),
  Row("Dallas",  7, "0.000406"),
  Row("Dallas",  8, "0.000476"),
  Row("Dallas",  9, "0.000388"),
  Row("Dallas", 10, "0.000458"), 
  Row("Dallas", 11, "0.000447"),
  Row("Dallas", 12, "0.000504"), 
  Row("Los Angeles",  1, "0.000181"),
  Row("Los Angeles",  2, "0.000170"),
  Row("Los Angeles",  3, "0.000178"), 
  Row("Los Angeles",  4, "0.000179"), 
  Row("Los Angeles",  5, "0.000199"),
  Row("Los Angeles",  6, "0.000176"),
  Row("Los Angeles",  7, "0.000208"),
  Row("Los Angeles",  8, "0.000192"), 
  Row("Los Angeles",  9, "0.000182"), 
  Row("Los Angeles", 10, "0.000205"),
  Row("Los Angeles", 11, "0.000192"), 
  Row("Los Angeles", 12, "0.000215"),
  Row("Philadelphia",  1, "0.000332"),
  Row("Philadelphia",  2, "0.000265"), 
  Row("Philadelphia",  3, "0.000276"),
  Row("Philadelphia",  4, "0.000297"), 
  Row("Philadelphia",  5, "0.000340"),
  Row("Philadelphia",  6, "0.000325"),
  Row("Philadelphia",  7, "0.000343"),
  Row("Philadelphia",  8, "0.000358"),
  Row("Philadelphia",  9, "0.000328"),
  Row("Philadelphia", 10, "0.000365"),
  Row("Philadelphia", 11, "0.000348"), 
  Row("Philadelphia", 12, "0.000347"))
assert(expectedResults == results, "the robberies by city data is incorrect")

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC ## References
// MAGIC 
// MAGIC The crime data used in this notebook comes from the following locations:
// MAGIC 
// MAGIC | City          | Original Data 
// MAGIC | ------------- | -------------
// MAGIC | Boston        | <a href="https://data.boston.gov/group/public-safety" target="_blank">https&#58;//data.boston.gov/group/public-safety</a>
// MAGIC | Chicago       | <a href="https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-present/ijzp-q8t2" target="_blank">https&#58;//data.cityofchicago.org/Public-Safety/Crimes-2001-to-present/ijzp-q8t2</a>
// MAGIC | Dallas        | <a href="https://www.dallasopendata.com/Public-Safety/Police-Incidents/tbnj-w5hb/data" target="_blank">https&#58;//www.dallasopendata.com/Public-Safety/Police-Incidents/tbnj-w5hb/data</a>
// MAGIC | Los Angeles   | <a href="https://data.lacity.org/A-Safe-City/Crime-Data-From-2010-to-Present/y8tr-7khq" target="_blank">https&#58;//data.lacity.org/A-Safe-City/Crime-Data-From-2010-to-Present/y8tr-7khq</a>
// MAGIC | New Orleans   | <a href="https://data.nola.gov/Public-Safety-and-Preparedness/Electronic-Police-Report-2016/4gc2-25he/data" target="_blank">https&#58;//data.nola.gov/Public-Safety-and-Preparedness/Electronic-Police-Report-2016/4gc2-25he/data</a>
// MAGIC | New York      | <a href="https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Historic/qgea-i56i" target="_blank">https&#58;//data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Historic/qgea-i56i</a>
// MAGIC | Philadelphia  | <a href="https://www.opendataphilly.org/dataset/crime-incidents" target="_blank">https&#58;//www.opendataphilly.org/dataset/crime-incidents</a>
