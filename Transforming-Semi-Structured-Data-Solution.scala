// Databricks notebook source
// MAGIC %md
// MAGIC # Querying JSON & Hierarchical Data with DataFrames
// MAGIC 
// MAGIC Apache Spark&trade; and Databricks&reg; make it easy to work with hierarchical data, such as nested JSON records.
// MAGIC 
// MAGIC ## In this lesson you:
// MAGIC * Use DataFrames to query JSON data.
// MAGIC * Query nested structured data.
// MAGIC * Query data containing array columns.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Getting Started
// MAGIC 
// MAGIC Mount your test data sets by executing the following cell.

// COMMAND ----------

if (!dbutils.fs.mounts.map(mnt => mnt.mountPoint).contains("/mnt/training"))
  dbutils.fs.mount(
    source = "s3a://AKIAJBRYNXGHORDHZB4A:a0BzE1bSegfydr3%2FGE3LSPM6uIV5A4hOUfpH8aFF@databricks-corp-training/common",
    mountPoint = "/mnt/training")    

println("Data from s3 is ready to use")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC %md
// MAGIC <iframe  
// MAGIC src="//fast.wistia.net/embed/iframe/rw260crs45?videoFoam=true"
// MAGIC style="border:1px solid #1cb1c2;"
// MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
// MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
// MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
// MAGIC <div>
// MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/rw260crs45?seo=false">
// MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC ## Examining the Contents of a JSON file
// MAGIC 
// MAGIC JSON is a common file format used in big data applications and in data lakes (or large stores of diverse data).  File formats such as JSON arise out of a number of data needs.  For instance, what if:
// MAGIC <br>
// MAGIC * Your schema, or the structure of your data, changes over time?
// MAGIC * You need nested fields like an array with many values or an array of arrays?
// MAGIC * You don't know how you're going use your data yet, so you don't want to spend time creating relational tables?
// MAGIC 
// MAGIC The popularity of JSON is largely due to the fact that JSON allows for nested, flexible schemas.
// MAGIC 
// MAGIC This lesson uses the `DatabricksBlog` table, which is backed by JSON file `dbfs:/mnt/training/databricks-blog.json`. If you examine the raw file, notice it contains compact JSON data. There's a single JSON object on each line of the file; each object corresponds to a row in the table. Each row represents a blog post on the <a href="https://databricks.com/blog" target="_blank">Databricks blog</a>, and the table contains all blog posts through August 9, 2017.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC %md
// MAGIC 
// MAGIC <iframe  
// MAGIC src="//fast.wistia.net/embed/iframe/sffliavw5s?videoFoam=true"
// MAGIC style="border:1px solid #1cb1c2;"
// MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
// MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
// MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
// MAGIC <div>
// MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/sffliavw5s?seo=false">
// MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
// MAGIC </div>

// COMMAND ----------

// MAGIC %fs head dbfs:/mnt/training/databricks-blog.json

// COMMAND ----------

// MAGIC %md
// MAGIC Create a DataFrame out of the syntax introduced in the previous lesson:

// COMMAND ----------

val databricksBlogDF = spark.read.option("inferSchema","true").option("header","true").json("/mnt/training/databricks-blog.json")

// COMMAND ----------

// MAGIC %md
// MAGIC Take a look at the schema by invoking `printSchema` method.

// COMMAND ----------

databricksBlogDF.printSchema
display(databricksBlogDF)

// COMMAND ----------

// MAGIC %md
// MAGIC Run a query to view the contents of the table.
// MAGIC 
// MAGIC Notice:
// MAGIC * The `authors` column is an array containing one or more author names.
// MAGIC * The `categories` column is an array of one or more blog post category names.
// MAGIC * The `dates` column contains nested fields `createdOn`, `publishedOn` and `tz`.

// COMMAND ----------

display(databricksBlogDF.select($"authors",$"categories",$"dates",$"content"))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Nested Data
// MAGIC 
// MAGIC Think of nested data as columns within columns. 
// MAGIC 
// MAGIC For instance, look at the `dates` column.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC %md
// MAGIC <iframe  
// MAGIC src="//fast.wistia.net/embed/iframe/mem8yosi3l?videoFoam=true"
// MAGIC style="border:1px solid #1cb1c2;"
// MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
// MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
// MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
// MAGIC <div>
// MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/mem8yosi3l?seo=false">
// MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
// MAGIC </div>

// COMMAND ----------

val datesDF = databricksBlogDF.select($"dates")
display(datesDF)

// COMMAND ----------

// MAGIC %md
// MAGIC Pull out a specific subfield with `.` (object) notation.

// COMMAND ----------

display(databricksBlogDF.select($"dates.createdOn", $"dates.publishedOn"))

// COMMAND ----------

// MAGIC %md
// MAGIC Create a DataFrame, `databricksBlog2DF` that contains the original columns plus the new `publishedOn` column obtained
// MAGIC from flattening the dates column.

// COMMAND ----------

val databricksBlog2DF = databricksBlogDF.withColumn("publishedOn",$"dates.publishedOn")

// COMMAND ----------

// MAGIC %md
// MAGIC With this temporary view, apply the `printSchema` method to check its schema and confirm the timestamp conversion.

// COMMAND ----------

databricksBlog2DF.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC Both `createdOn` and `publishedOn` are stored as strings.
// MAGIC 
// MAGIC Cast those values to SQL timestamps:
// MAGIC 
// MAGIC In this case, use a single `select` method to:
// MAGIC 0. Cast `dates.publishedOn` to a `timestamp` data type
// MAGIC 0. "Flatten" the `dates.publishedOn` column to just `publishedOn`

// COMMAND ----------

import org.apache.spark.sql.functions.to_timestamp
display(databricksBlogDF.select($"title",to_timestamp($"dates.publishedOn","yyyy-MM-dd") as "publishedOn"))

// COMMAND ----------

// MAGIC %md
// MAGIC Create another DataFrame, `databricksBlog2DF` that contains the original columns plus the new `publishedOn` column obtained
// MAGIC from flattening the dates column.

// COMMAND ----------

val databricksBlog2DF = databricksBlogDF.withColumn("publishedOn", to_timestamp($"dates.publishedOn","yyyy-MM-dd")) 
display(databricksBlog2DF)

// COMMAND ----------

// MAGIC %md
// MAGIC With this temporary view, apply the `printSchema` method to check its schema and confirm the timestamp conversion.

// COMMAND ----------

databricksBlog2DF.printSchema

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC Since the dates are represented by a `timestamp` data type, we need to convert to a data type that allows `<` and `>`-type comparison operations in order to query for articles within certain date ranges (such as a list of all articles published in 2013). This is accopmplished by using the `to_date` function in Scala or Python.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> See the Spark documentation on <a href="https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$" target="_blank">built-in functions</a>, for a long list of date-specific functions.

// COMMAND ----------

import org.apache.spark.sql.functions.{to_date, year}
val resultDF = databricksBlog2DF
  .select($"title", to_date($"publishedOn","MMM dd, yyyy") as "date", $"link")
  .filter(year($"publishedOn") === "2013")
  .orderBy($"publishedOn")

display(resultDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Array Data
// MAGIC 
// MAGIC The DataFrame also contains array columns. 
// MAGIC 
// MAGIC Easily determine the size of each array using the built-in `size(..)` function with array columns.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC %md
// MAGIC <iframe  
// MAGIC src="//fast.wistia.net/embed/iframe/u5tl615jit?videoFoam=true"
// MAGIC style="border:1px solid #1cb1c2;"
// MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
// MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
// MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
// MAGIC <div>
// MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/u5tl615jit?seo=false">
// MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
// MAGIC </div>

// COMMAND ----------

import org.apache.spark.sql.functions.size
display(databricksBlogDF.select(size($"authors"), $"authors"))

// COMMAND ----------

// MAGIC %md
// MAGIC Pull the first element from the array `authors` using an array subscript operator.
// MAGIC 
// MAGIC For example, in Scala, the 0th element of array `authors` is `authors(0)`
// MAGIC whereas, in Python, the 0th element of `authors` is `authors[0]`.

// COMMAND ----------

display(databricksBlogDF.select($"authors"(0) as "primaryAuthor"))

// COMMAND ----------

// MAGIC %md
// MAGIC ### Explode
// MAGIC 
// MAGIC The `explode` method allows you to split an array column into multiple rows, copying all the other columns into each new row. 
// MAGIC 
// MAGIC For example, split the column `authors` into the column `author`, with one author per row.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC %md
// MAGIC <iframe  
// MAGIC src="//fast.wistia.net/embed/iframe/ka2sd6jk1j?videoFoam=true"
// MAGIC style="border:1px solid #1cb1c2;"
// MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
// MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
// MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
// MAGIC <div>
// MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/ka2sd6jk1j?seo=false">
// MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
// MAGIC </div>

// COMMAND ----------

import org.apache.spark.sql.functions.explode
display(databricksBlogDF.select($"title", $"authors", explode($"authors") as "author", $"link"))

// COMMAND ----------

// MAGIC %md
// MAGIC It's more obvious to restrict the output to articles that have multiple authors, and then sort by the title.

// COMMAND ----------

val databricksBlog2DF = databricksBlogDF
  .select($"title", $"authors", explode($"authors") as "author", $"link")
  .filter(size($"authors") > 1)
  .orderBy($"title")
  
display(databricksBlog2DF)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Exercise 1
// MAGIC 
// MAGIC Identify all the articles written or co-written by Michael Armbrust.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### Step 1
// MAGIC 
// MAGIC Starting with the `databricksBlogDF` DataFrame, create a DataFrame called `articlesByMichaelDF` where:
// MAGIC 0. Michael Armbrust is the author.
// MAGIC 0. The data set contains the column `title` as the first column (it may contain others).
// MAGIC 0. It contains only one record per article.
// MAGIC 
// MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** See the Spark documentation on <a href="https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$" target="_blank">built-in functions</a>.  
// MAGIC 
// MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Include the column `authors` in your view to help you debug your solution.

// COMMAND ----------

val articlesByMichaelDF = databricksBlogDF
  .select($"title", explode($"authors") as "author")
  .filter($"author" === "Michael Armbrust")

// COMMAND ----------


// TEST - Run this cell to test your solution.

lazy val resultsCount = articlesByMichaelDF.count()

assert( 3 == resultsCount, "DF-L5-articlesByMichael-count failed")  

lazy val results = articlesByMichaelDF.collect()
println(results(0))
assert(Row("Spark SQL: Manipulating Structured Data Using Apache Spark")(0) == results(0)(0), "DF-L5-articlesByMichael-first row is not correct")
assert(Row("Exciting Performance Improvements on the Horizon for Spark SQL")(0) == results(1)(0), "DF-L5-articlesByMichael second row is not correct")
assert(Row("Spark SQL Data Sources API: Unified Data Access for the Apache Spark Platform")(0) == results(2)(0), "DF-L5-articlesByMichael third row is not correct")

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 2
// MAGIC Show the list of Michael Armbrust's articles using the Databricks built in display() function.

// COMMAND ----------

display(articlesByMichaelDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Exercise 2
// MAGIC 
// MAGIC Identify the complete set of categories used in the Databricks blog articles.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 1
// MAGIC 
// MAGIC Starting with the `databricksBlogDF` DataFrame, create another DataFrame called `uniqueCategoriesDF` where:
// MAGIC 0. The data set contains the one column `category` (and no others).
// MAGIC 0. This list of categories should be unique.

// COMMAND ----------

import org.apache.spark.sql.functions._
val uniqueCategoriesDF = databricksBlogDF
  .select(explode($"categories") as "category")
  .orderBy(asc("category"))
  .distinct

// COMMAND ----------


// TEST - Run this cell to test your solution.

lazy val resultsCount =  uniqueCategoriesDF.count()

assert(12 == resultsCount, "DF-L5-uniqueCategories-count")

lazy val results = uniqueCategoriesDF.collect()
assert(Row("Announcements") == results(0), "DF-L5-uniqueCategories-0")
assert(Row("Apache Spark") == results(1), "DF-L5-uniqueCategories-1")
assert(Row("Company Blog") == results(2), "DF-L5-uniqueCategories-2")

assert(Row("Platform") == results(9), "DF-L5-uniqueCategories-9")
assert(Row("Product") == results(10), "DF-L5-uniqueCategories-10")
assert(Row("Streaming") == results(11), "DF-L5-uniqueCategories-11")

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 2
// MAGIC Show the complete list of categories.

// COMMAND ----------

display(uniqueCategoriesDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Exercise 3
// MAGIC 
// MAGIC Count how many times each category is referenced in the Databricks blog.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### Step 1
// MAGIC 
// MAGIC Starting with the `databricksBlogDF` DataFrame, create another DataFrame called `totalArticlesByCategoryDF` where:
// MAGIC 0. The new DataFrame contains two columns, `category` and `total`.
// MAGIC 0. The `category` column is a single, distinct category (similar to the last exercise).
// MAGIC 0. The `total` column is the total number of articles in that category.
// MAGIC 0. Order by `category`.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Because articles can be tagged with multiple categories, the sum of the totals adds up to more than the total number of articles.

// COMMAND ----------

// TODO

import org.apache.spark.sql.functions.count
val totalArticlesByCategoryDF = databricksBlogDF
  .select(explode($"categories") as "category", size($"categories"))
  .orderBy(asc("category"))
  .groupBy($"category")
  .agg(count("category"))

// COMMAND ----------

// TEST - Run this cell to test your solution.

lazy val results = totalArticlesByCategoryDF.count()
assert(12 == results, "DF-L5-articlesByCategory-count")

println("Tests passed!")

// COMMAND ----------

// TEST - Run this cell to test your solution.

lazy val results = totalArticlesByCategoryDF.collect()
assert(Row("Announcements",72) == results(0), "DF-L5-articlesByCategory-0")
assert(Row("Apache Spark",132) == results(1), "DF-L5-articlesByCategory-1")
assert(Row("Company Blog",224) == results(2), "DF-L5-articlesByCategory-2")

assert(Row("Platform",4) == results(9), "DF-L5-articlesByCategory-9")
assert(Row("Product",83) == results(10), "DF-L5-articlesByCategory-10")
assert(Row("Streaming",21) == results(11), "DF-L5-articlesByCategory-11")

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 2
// MAGIC Display the totals of each category using the built in Databricks display() function. (should be ordered by `category`).

// COMMAND ----------

display(totalArticlesByCategoryDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Summary
// MAGIC 
// MAGIC * Spark DataFrames allows you to query and manipulate structured and semi-structured data.
// MAGIC * Spark DataFrames built-in functions provide powerful primitives for querying complex schemas.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Review Questions
// MAGIC **Q:** What is the syntax for accessing nested columns?  
// MAGIC **A:** Use the dot notation:`select($"dates.publishedOn")`
// MAGIC 
// MAGIC **Q:** What is the syntax for accessing the first element in an array?  
// MAGIC **A:** Use the [subscript] notation: `select($"authors"(0))`
// MAGIC 
// MAGIC **Q:** What is the syntax for expanding an array into multiple rows?  
// MAGIC **A:** Use the explode method:  `select(explode($"authors") as "Author")`

// COMMAND ----------

// MAGIC %md
// MAGIC ## Additional Topics & Resources
// MAGIC 
// MAGIC * <a href="http://spark.apache.org/docs/latest/sql-programming-guide.html" target="_blank">Spark SQL, DataFrames and Datasets Guide</a>
