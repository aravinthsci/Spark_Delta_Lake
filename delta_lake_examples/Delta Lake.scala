%AddJar https://repo1.maven.org/maven2/io/delta/delta-core_2.11/0.4.0/delta-core_2.11-0.4.0.jar

import org.apache.spark.sql.delta.DeltaLog
import io.delta.tables._
import org.apache.spark.sql.functions._ 
import org.apache.spark.sql.{SaveMode, SparkSession, DataFrame}

  def createTable(data: DataFrame, tableName: String ): Unit = {
    data
      .write
      .format("delta")
      .mode(SaveMode.Overwrite)
      .save("/opt/deltalake/" + tableName)
  }

 def readTable(tableName: String): DataFrame = {
   val df = spark
      .read
      .format("delta")
      .load("/opt/deltalake/" + tableName)
     df
  }

def updateDeltaTable(data: DataFrame, tableName: String, savemode: String): Unit = {
    data
      .write
      .format("delta")
      .mode(savemode)
      .save("/opt/deltalake/" + tableName)
  }

 def timeTravel(tableName: String, version: Int): DataFrame = {
   val df = spark
      .read
      .format("delta")
      .option("versionAsOf", version)
      .load("/opt/deltalake/" + tableName)
     df
  }

 def addColumn(data: DataFrame,tableName: String): Unit = {
  data
      .write
      .format("delta")
      .mode("overwrite")
      .option("mergeSchema", "true")
      .save("/opt/deltalake/" + tableName)
  }

 def getLastestHistory(tableName: String): DataFrame = {
     val deltaTable = DeltaTable.forPath(spark, "/opt/deltalake/" + tableName)
     val lastOperationDF  = deltaTable.history(1) 
     lastOperationDF 
  }

 def getHistory(tableName: String): DataFrame = {
     val deltaTable = DeltaTable.forPath(spark, "/opt/deltalake/" + tableName)
     val fullHistoryDF = deltaTable.history() 
     fullHistoryDF
  }

import io.delta.tables._

val deltaTable = DeltaTable.forPath(spark, pathToTable)

val fullHistoryDF = deltaTable.history() 

val df = spark.read.option("header",true).csv("Sale_test.csv")

val modifiedDF = df.withColumn("date", date_format($"created_at", "yyyy-MM-dd"))

createTable(modifiedDF, "sales")

val sales_df = readTable("sales")

sales_df.count()

val data = spark.range(0, 5).toDF("no")
createTable(data, "numbers")

val moreData = spark.range(20, 25).toDF("no")
updateDeltaTable(moreData, "numbers", "overwrite")

val moreMoreData = spark.range(26, 30)
updateDeltaTable(moreData, "numbers", "append")

val no_df = readTable("numbers")
no_df.show()

val version_0_df = timeTravel("numbers", 0)
version_0_df .show()

val version_1_df = timeTravel("numbers", 1)
version_1_df.show()

val version_2_df = timeTravel("numbers", 2)
version_2_df.show()

val new_df = version_2_df.withColumn("new_col",lit("abc"))
new_df.show()

addColumn(new_df, "numbers")

val latest_df = readTable("numbers")
latest_df.show()

val numbers_table_his = getHistory("numbers")
numbers_table_his.show()

val numbers_table_lat_his = getLastestHistory("numbers")
numbers_table_lat_his.show()


