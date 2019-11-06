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

val data = readTable("sales")
data.show()

val deltaTable = DeltaTable.forPath(spark, "/opt/deltalake/sales")
deltaTable.delete(condition = "date = '1970-01-01'")  

deltaTable.updateExpr("date = '1970-01-06'", Map("created_at" -> "'ara'") ) 

readTable("sales").show()

deltaTable.update(
  condition = expr("date = '1970-01-06'"),
  set = Map("created_at" -> expr("'raja'")))

readTable("sales").show()

val newData = spark.range(500, 503).toDF

    deltaTable.as("oldData")
      .merge(
        newData.as("newData"),
        "oldData.id = newData.id")
      .whenMatched
      .update(Map("id" -> col("newData.id")))
      .whenNotMatched
      .insert(Map("id" -> col("newData.id")))
      .execute()

readTable("numbers").show()


