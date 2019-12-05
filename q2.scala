// Databricks notebook source
// Analyzing a Large Graph with Spark/Scala on Databricks

// STARTER CODE 
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spark.implicits._

// COMMAND ----------

// STARTER CODE
// Definfing the data schema
val customSchema = StructType(Array(StructField("answerer", IntegerType, true), StructField("questioner", IntegerType, true),
    StructField("timestamp", LongType, true)))

// COMMAND ----------

// STARTER CODE
val df = spark.read
   .format("com.databricks.spark.csv")
   .option("header", "false") // Use first line of all files as header
   .option("nullValue", "null")
   .schema(customSchema)
   .load("/FileStore/tables/mathoverflow.csv")
   .withColumn("date", from_unixtime($"timestamp"))
   .drop($"timestamp")

// COMMAND ----------

//display(df)

df.show()
//df.count()

// COMMAND ----------

// PART 1: Remove the pairs where the questioner and the answerer are the same person.
// ALL THE SUBSEQUENT OPERATIONS ARE PERFORMED ON THIS FILTERED DATA

//df.dropDuplicates("answerer", "questioner").show()
//df.distinct().show()

val filterDF = df
  .filter($"answerer" =!= $"questioner")

//filterDF.count()
filterDF.show()

// COMMAND ----------

// PART 2: The top-3 individuals who answered the most number of questions - sorted in descending order - if tie, the one with lower node-id gets listed first : the nodes with the highest out-degrees.

val answered = filterDF.select($"answerer", $"date")
                .groupBy($"answerer")
                .agg(count($"date") as "questions_answered")
                .sort($"questions_answered".desc, $"answerer".asc)

//display(countDistinctDF)
answered.show(3)


// COMMAND ----------

// PART 3: The top-3 individuals who asked the most number of questions - sorted in descending order - if tie, the one with lower node-id gets listed first : the nodes with the highest in-degree.

val questioned = filterDF.select($"questioner", $"date")
                    .groupBy($"questioner")
                    .agg(count($"date") as "questions_asked")
                    .sort($"questions_asked".desc, $"questioner".asc)

questioned.show(3)

// COMMAND ----------

// PART 4: The top-5 most common asker-answerer pairs - sorted in descending order - if tie, the one with lower value node-id in the first column (u->v edge, u value) gets listed first.

filterDF.select($"answerer", $"questioner", $"date")
  .groupBy($"answerer", $"questioner")
  .agg(count($"date") as "count")
  .sort($"count".desc, $"answerer".asc, $"questioner".asc)
  .show(5)

// COMMAND ----------

// PART 5: Number of interactions (questions asked/answered) over the months of September-2010 to December-2010 (i.e. from September 1, 2010 to December 31, 2010). List the entries by month from September to December.

// Reference: https://www.obstkel.com/blog/spark-sql-date-functions
// Read in the data and extract the month and year from the date column.
// Hint: Check how we extracted the date from the timestamp.

filterDF.withColumn("month", month(col("date")))
        .withColumn("year", year(col("date")))
        .filter($"year" === 2010)
        .filter($"month" >= 9)
        .select($"month", $"date", $"year" )
        .groupBy($"month")
        .agg(count($"date") as "total_interactions")
        .sort($"month".asc)
        .show()

// COMMAND ----------

// PART 6: List the top-3 individuals with the maximum overall activity, i.e. total questions asked and questions answered.

answered.union(questioned)
        .select(expr("answerer as userID"), expr("questions_answered as qa"))       
        .groupBy($"userID")
        .agg(sum($"qa") as "total_activity")
        .sort($"total_activity".desc, $"userID".asc)
        .show(3)
