// Databricks notebook source
println("Hello World! in Scala")

// COMMAND ----------

val file = sc.textFile("/FileStore/tables/covid_19_data.txt")
println("Number of lines in File: "+file.count())

// COMMAND ----------

var i =1
for (line <- file.take(3).toList) {
  println("Line Number "+i+" :"+line)
  i+=1
}

// COMMAND ----------

val file = sc.textFile("/FileStore/tables/covid_19_data.txt")
for(line <- file.collect){
  println(line)
}

// COMMAND ----------

import org.apache.spark.util.SizeEstimator
val file = sc.textFile("/FileStore/tables/covid_19_data.txt")
println(SizeEstimator.estimate(file))

// COMMAND ----------

val textFile = sc.textFile("/FileStore/tables/covid_19_data.txt")
val linesWithFujian = textFile.filter(line => line.contains("Fujian"))
textFile.filter(line => line.contains("Fujian")).count()


// COMMAND ----------

val textFile = sc.textFile("/FileStore/tables/covid_19_data.txt")
val linesWithGansu = textFile.filter(line => line.contains("Gansu"))
textFile.filter(line => line.contains("Gansu")).count()

// COMMAND ----------

val textFile = sc.textFile("/FileStore/tables/covid_19_data.txt")
              .flatMap(_.split("\\W+"))
              .filter(!_.isEmpty)
              .map((_,1))
              .reduceByKey(_ + _)
              .takeOrdered(10)(Ordering[Int].reverse.on(_._2))

val df = sc.parallelize(textFile).toDF("Word","Frequency")
df.show()

// COMMAND ----------

val textFile = sc.textFile("/FileStore/tables/covid_19_data.txt")
val allWords = textFile.flatMap(_.split("\\W+"))
val words = allWords.filter(!_.isEmpty)
val pairs = words.map((_,1))
val reducedByKey = pairs.reduceByKey(_ + _)
val top10Words= reducedByKey.takeOrdered(10)(Ordering[Int].reverse.on(_._2))
val df_1 = sc.parallelize(top10Words).toDF("Word","Frequency")
df_1.show()

// COMMAND ----------


