%AddJar https://repo1.maven.org/maven2/io/delta/delta-core_2.11/0.4.0/delta-core_2.11-0.4.0.jar

import org.apache.spark.sql.delta.DeltaLog
import io.delta.tables._
import org.apache.spark.sql.functions._ 
import org.apache.spark.sql.{SaveMode, SparkSession, DataFrame}

  def createPartitionedTable(data: DataFrame, tableName: String, Keys: String): Unit = {
  data
      .write
      .partitionBy(Keys)
      .format("delta")
      .mode("overwrite")
      .option("mergeSchema", "true")
      .save("/opt/partitioned_lake/" + tableName)
  }

def updatePartitionedTable(data: DataFrame, tableName: String, condition: String): Unit = {
  data
      .write
      .format("delta")
      .option("replaceWhere", condition)
      .mode("overwrite")
      .option("mergeSchema", "true")
      .save("/opt/partitioned_lake/" + tableName)
  }

 def readTable(tableName: String): DataFrame = {
   val df = spark
      .read
      .format("delta")
      .load("/opt/partitioned_lake/" + tableName)
     df
  }

val sales_df = spark.read.option("header",true).csv("Sale_test.csv")

val modifiedDF = sales_df.withColumn("date", date_format($"created_at", "yyyy-MM-dd"))

modifiedDF.createOrReplaceTempView("sales")
modifiedDF.show()

createPartitionedTable(modifiedDF, "sales", "date")

val res_df = spark.sql("select id, "+
"product_id,created_at,date ,case when date='1970-01-01' then units*10 else units end as units from sales where date='1970-01-01'")
res_df.show()

updatePartitionedTable(res_df,"sales", "date= '1970-01-01'")

readTable("sales").show(false)


