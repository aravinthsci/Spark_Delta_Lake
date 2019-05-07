package com.aravinth.datalake

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.lit

import org.apache.log4j.{Level, Logger}


object deltaApp extends App {

  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)
  rootLogger.setLevel(Level.INFO)


  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val Spark = SparkSession.builder()
    .appName("Spark Delta example")
    .master("local")
    .config("spark.executor.instances", "2")
    .getOrCreate()



  //Reading a file
  val df = Spark.read.option("header",true).csv("C:\\Users\\Aravinth\\Datalake\\products.csv")

  //Creating a table
  df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("/delta-table/product")

  //Reading a table

  val df1 = Spark.read.format("delta").load("/delta-table/product")
  df1.show()

  /*+---+---------+---------+-----+----------------+---------+-----+-----+-------+------+
| id|firstName| lastName|house|          street|     city|state|  zip|   prod|   tag|
+---+---------+---------+-----+----------------+---------+-----+-----+-------+------+
|  1|  Micheal|   Adkins|   19|    Ofuca Avenue| Kopewevi|   PA|77774|   Misc| GREEN|
|  2|   Rachel|    Parks|   19|     Itmer Manor| Wejitcan|   AR|64675|Desktop|YELLOW|
|  3|   Julian|    Olson|   47| Pewih Boulevard| Lithejev|   NH|27953|Desktop|  BLUE|
|  4|     Carl|  Kennedy|   62|  Pekibu Highway| Jerhivme|   WV|51677|   Misc|  BLUE|
|  5|     Earl|Armstrong|   47|  Rejo Extension| Febugiro|   MO|05472| Laptop|YELLOW|
|  6|    Della|Armstrong|   25|    Febro Street| Wivgufiv|   MD|28380|Desktop|  BLUE|
|  7|   Milton|   George|   44|    Febos Circle|  Nuwacof|   MO|58670| Laptop|   RED|
|  8|  Jeffery|   Hughes|   39|      Zivve Park|  Dirhako|   NJ|03544|  Phone|   RED|
|  9|     Lura|   Thomas|   21|       Pika Lane|  Fasidwu|   ID|84200| Tablet|   RED|
| 10|      Ora|    Green|   20|Hevhit Extension|  Gounnob|   NJ|23654| Tablet| WHITE|
| 11|   Amelia|  Gardner|   35|       Veko View|  Jiwpihe|   NH|14911| Tablet| WHITE|
| 12|  Barbara| Sullivan|   28|     Zinkun Lane|  Licsuso|   LA|93342|   Misc| GREEN|
| 13|  Timothy|   Butler|   22|        Ibir Key| Mamimasi|   SD|28341|   Misc|   RED|
| 14| Winifred|    Greer|   38|     Maofo Plaza| Wutrapja|   NV|14020| Tablet| GREEN|
| 15| Mathilda|  McGuire|   44|      Pomrij Key| Ukijajis|   WA|75335| Tablet|YELLOW|
| 16|    Roger|  Jackson|   33|     Zeglaf Loop| Ahwajnes|   FL|05185|Desktop|YELLOW|
| 17|    Allen|   Norris|   43|      Wenul Pike|  Suparot|   PA|65574|   Misc|  BLUE|
| 18|    Lloyd|  Coleman|   63|       Cuah Lane|Pigpilmal|   WA|23346| Tablet| GREEN|
| 19|  Barbara|   Powell|   20|      Izvo Grove| Itauvtes|   OH|55071|   Misc|YELLOW|
| 20|   Hattie|  Douglas|   33| Ilaide Turnpike| Bijilrol|   HI|51512|   Misc| GREEN|
+---+---------+---------+-----+----------------+---------+-----+-----+-------+------+*/


  //Adding column to table

  val newDF = df.withColumn("Country",lit("India"))

  newDF.write.format("delta").mode("overwrite").option("mergeSchema", "true").save("/delta-table/product")


  //New Table
  val df2 = Spark.read.format("delta").load("/delta-table/product")
  df2.show()

  /*+---+---------+---------+-----+----------------+---------+-----+-----+-------+------+-------+
| id|firstName| lastName|house|          street|     city|state|  zip|   prod|   tag|Country|
+---+---------+---------+-----+----------------+---------+-----+-----+-------+------+-------+
|  1|  Micheal|   Adkins|   19|    Ofuca Avenue| Kopewevi|   PA|77774|   Misc| GREEN|  India|
|  2|   Rachel|    Parks|   19|     Itmer Manor| Wejitcan|   AR|64675|Desktop|YELLOW|  India|
|  3|   Julian|    Olson|   47| Pewih Boulevard| Lithejev|   NH|27953|Desktop|  BLUE|  India|
|  4|     Carl|  Kennedy|   62|  Pekibu Highway| Jerhivme|   WV|51677|   Misc|  BLUE|  India|
|  5|     Earl|Armstrong|   47|  Rejo Extension| Febugiro|   MO|05472| Laptop|YELLOW|  India|
|  6|    Della|Armstrong|   25|    Febro Street| Wivgufiv|   MD|28380|Desktop|  BLUE|  India|
|  7|   Milton|   George|   44|    Febos Circle|  Nuwacof|   MO|58670| Laptop|   RED|  India|
|  8|  Jeffery|   Hughes|   39|      Zivve Park|  Dirhako|   NJ|03544|  Phone|   RED|  India|
|  9|     Lura|   Thomas|   21|       Pika Lane|  Fasidwu|   ID|84200| Tablet|   RED|  India|
| 10|      Ora|    Green|   20|Hevhit Extension|  Gounnob|   NJ|23654| Tablet| WHITE|  India|
| 11|   Amelia|  Gardner|   35|       Veko View|  Jiwpihe|   NH|14911| Tablet| WHITE|  India|
| 12|  Barbara| Sullivan|   28|     Zinkun Lane|  Licsuso|   LA|93342|   Misc| GREEN|  India|
| 13|  Timothy|   Butler|   22|        Ibir Key| Mamimasi|   SD|28341|   Misc|   RED|  India|
| 14| Winifred|    Greer|   38|     Maofo Plaza| Wutrapja|   NV|14020| Tablet| GREEN|  India|
| 15| Mathilda|  McGuire|   44|      Pomrij Key| Ukijajis|   WA|75335| Tablet|YELLOW|  India|
| 16|    Roger|  Jackson|   33|     Zeglaf Loop| Ahwajnes|   FL|05185|Desktop|YELLOW|  India|
| 17|    Allen|   Norris|   43|      Wenul Pike|  Suparot|   PA|65574|   Misc|  BLUE|  India|
| 18|    Lloyd|  Coleman|   63|       Cuah Lane|Pigpilmal|   WA|23346| Tablet| GREEN|  India|
| 19|  Barbara|   Powell|   20|      Izvo Grove| Itauvtes|   OH|55071|   Misc|YELLOW|  India|
| 20|   Hattie|  Douglas|   33| Ilaide Turnpike| Bijilrol|   HI|51512|   Misc| GREEN|  India|
+---+---------+---------+-----+----------------+---------+-----+-----+-------+------+-------+
only showing top 20 rows*/



//Time Travel

  val timeTravelDF_1 = Spark.read.format("delta").option("versionAsOf", 0).load("/delta-table/product")
  timeTravelDF_1.show()

  /*+---+---------+---------+-----+----------------+---------+-----+-----+-------+------+
| id|firstName| lastName|house|          street|     city|state|  zip|   prod|   tag|
+---+---------+---------+-----+----------------+---------+-----+-----+-------+------+
|  1|  Micheal|   Adkins|   19|    Ofuca Avenue| Kopewevi|   PA|77774|   Misc| GREEN|
|  2|   Rachel|    Parks|   19|     Itmer Manor| Wejitcan|   AR|64675|Desktop|YELLOW|
|  3|   Julian|    Olson|   47| Pewih Boulevard| Lithejev|   NH|27953|Desktop|  BLUE|
|  4|     Carl|  Kennedy|   62|  Pekibu Highway| Jerhivme|   WV|51677|   Misc|  BLUE|
|  5|     Earl|Armstrong|   47|  Rejo Extension| Febugiro|   MO|05472| Laptop|YELLOW|
|  6|    Della|Armstrong|   25|    Febro Street| Wivgufiv|   MD|28380|Desktop|  BLUE|
|  7|   Milton|   George|   44|    Febos Circle|  Nuwacof|   MO|58670| Laptop|   RED|
|  8|  Jeffery|   Hughes|   39|      Zivve Park|  Dirhako|   NJ|03544|  Phone|   RED|
|  9|     Lura|   Thomas|   21|       Pika Lane|  Fasidwu|   ID|84200| Tablet|   RED|
| 10|      Ora|    Green|   20|Hevhit Extension|  Gounnob|   NJ|23654| Tablet| WHITE|
| 11|   Amelia|  Gardner|   35|       Veko View|  Jiwpihe|   NH|14911| Tablet| WHITE|
| 12|  Barbara| Sullivan|   28|     Zinkun Lane|  Licsuso|   LA|93342|   Misc| GREEN|
| 13|  Timothy|   Butler|   22|        Ibir Key| Mamimasi|   SD|28341|   Misc|   RED|
| 14| Winifred|    Greer|   38|     Maofo Plaza| Wutrapja|   NV|14020| Tablet| GREEN|
| 15| Mathilda|  McGuire|   44|      Pomrij Key| Ukijajis|   WA|75335| Tablet|YELLOW|
| 16|    Roger|  Jackson|   33|     Zeglaf Loop| Ahwajnes|   FL|05185|Desktop|YELLOW|
| 17|    Allen|   Norris|   43|      Wenul Pike|  Suparot|   PA|65574|   Misc|  BLUE|
| 18|    Lloyd|  Coleman|   63|       Cuah Lane|Pigpilmal|   WA|23346| Tablet| GREEN|
| 19|  Barbara|   Powell|   20|      Izvo Grove| Itauvtes|   OH|55071|   Misc|YELLOW|
| 20|   Hattie|  Douglas|   33| Ilaide Turnpike| Bijilrol|   HI|51512|   Misc| GREEN|
+---+---------+---------+-----+----------------+---------+-----+-----+-------+------+*/

  val timeTravelDF_2 = Spark.read.format("delta").option("versionAsOf", 1).load("/delta-table/product")
  timeTravelDF_2.show()

  /*+---+---------+---------+-----+----------------+---------+-----+-----+-------+------+-------+
| id|firstName| lastName|house|          street|     city|state|  zip|   prod|   tag|Country|
+---+---------+---------+-----+----------------+---------+-----+-----+-------+------+-------+
|  1|  Micheal|   Adkins|   19|    Ofuca Avenue| Kopewevi|   PA|77774|   Misc| GREEN|  India|
|  2|   Rachel|    Parks|   19|     Itmer Manor| Wejitcan|   AR|64675|Desktop|YELLOW|  India|
|  3|   Julian|    Olson|   47| Pewih Boulevard| Lithejev|   NH|27953|Desktop|  BLUE|  India|
|  4|     Carl|  Kennedy|   62|  Pekibu Highway| Jerhivme|   WV|51677|   Misc|  BLUE|  India|
|  5|     Earl|Armstrong|   47|  Rejo Extension| Febugiro|   MO|05472| Laptop|YELLOW|  India|
|  6|    Della|Armstrong|   25|    Febro Street| Wivgufiv|   MD|28380|Desktop|  BLUE|  India|
|  7|   Milton|   George|   44|    Febos Circle|  Nuwacof|   MO|58670| Laptop|   RED|  India|
|  8|  Jeffery|   Hughes|   39|      Zivve Park|  Dirhako|   NJ|03544|  Phone|   RED|  India|
|  9|     Lura|   Thomas|   21|       Pika Lane|  Fasidwu|   ID|84200| Tablet|   RED|  India|
| 10|      Ora|    Green|   20|Hevhit Extension|  Gounnob|   NJ|23654| Tablet| WHITE|  India|
| 11|   Amelia|  Gardner|   35|       Veko View|  Jiwpihe|   NH|14911| Tablet| WHITE|  India|
| 12|  Barbara| Sullivan|   28|     Zinkun Lane|  Licsuso|   LA|93342|   Misc| GREEN|  India|
| 13|  Timothy|   Butler|   22|        Ibir Key| Mamimasi|   SD|28341|   Misc|   RED|  India|
| 14| Winifred|    Greer|   38|     Maofo Plaza| Wutrapja|   NV|14020| Tablet| GREEN|  India|
| 15| Mathilda|  McGuire|   44|      Pomrij Key| Ukijajis|   WA|75335| Tablet|YELLOW|  India|
| 16|    Roger|  Jackson|   33|     Zeglaf Loop| Ahwajnes|   FL|05185|Desktop|YELLOW|  India|
| 17|    Allen|   Norris|   43|      Wenul Pike|  Suparot|   PA|65574|   Misc|  BLUE|  India|
| 18|    Lloyd|  Coleman|   63|       Cuah Lane|Pigpilmal|   WA|23346| Tablet| GREEN|  India|
| 19|  Barbara|   Powell|   20|      Izvo Grove| Itauvtes|   OH|55071|   Misc|YELLOW|  India|
| 20|   Hattie|  Douglas|   33| Ilaide Turnpike| Bijilrol|   HI|51512|   Misc| GREEN|  India|
+---+---------+---------+-----+----------------+---------+-----+-----+-------+------+-------+*/


Spark.stop()


}
