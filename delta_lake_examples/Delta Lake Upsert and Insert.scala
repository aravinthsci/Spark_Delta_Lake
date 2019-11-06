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

val loan = Seq(
  (2, 1000, 1000.0, "TX"), // existing loan's paid_amnt updated, loan paid in full
  (3, 2000, 0.0, "CA"))    // new loan's details
  .toDF("loan_id", "funded_amnt", "paid_amnt", "addr_state")


createTable(loan,"loan")

readTable("loan").show()

val loanUpdates = Seq(
  (1, 1000, 1000.0, "TX"), // existing loan's paid_amnt updated, loan paid in full
  (3, 3000, 1.0, "CAA"))    // new loan's details
  .toDF("loan_id", "funded_amnt", "paid_amnt", "addr_state")


import io.delta.tables.DeltaTable
val deltaTable = DeltaTable.forPath(spark, "/opt/deltalake/loan")

deltaTable.alias("t").merge(
  loanUpdates.alias("s"), 
  "s.loan_id = t.loan_id")
  .whenMatched.updateAll()
  .whenNotMatched.insertAll()
  .execute()


readTable("loan").show()


