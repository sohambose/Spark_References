import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{explode, lower, round, split, sum}
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, StringType, StructType}

object DataSets_SparkMethods extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  //--------------------DATASETS ONLY-----------------------
  case class Book(value: String)

  //Create a SparkSession
  val spark = SparkSession
    .builder
    .appName("WordCount")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
    .getOrCreate()

  //-----Read each line of my book into an Dataset

  import spark.implicits._

  val input = spark.read.text("DataFiles/Book.txt").as[Book] //--as[Book] -reads into dataset

  //Explanation of the below code:
  //The above code has lines of text. Column name is "value"
  //split($"value", " ") --> Gets array of all words including blank spaces.
  //explode()- SELECT explode(array(10, 20));
  // 				10
  // 				20
  //Hence- Each row will now contain one word only. Name that column "word"
  //.filter($"word" =!= "")-- Filter out blanks
  val words = input
    .select(explode(split($"value", " ")).alias("word"))
    .filter($"word" =!= "")

  //Count up the occurrences of each word
  val wordCounts = words.groupBy("word").count()

  // Show the results
  //NOTE- show() method displays the dataset contents in tabular format.
  //wordCounts.show(wordCounts.count.toInt)
  //------------------------------------------------------------------------------


  //--------------------DATASETS COMBINED WITH RDD--------------------------------
  val bookRDD = spark.sparkContext.textFile("DataFiles/Book.txt") //--Read as RDD
  val wordsRDD = bookRDD.flatMap(x => x.split("\\W+")) //Use flatMap to split words
  val wordsDS = wordsRDD.toDS() //Then convert the RDD to Dataset for easy querying

  //Convert all words to lower. So that case does not count more..
  val lowercaseWordsDS = wordsDS.select(lower($"value").alias("word"))

  //Group by words and count
  val wordCountsDS = lowercaseWordsDS.groupBy("word").count()
  val wordCountsSortedDS = wordCountsDS.sort("count")

  //wordCountsSortedDS.show(wordCountsSortedDS.count.toInt)
  //------------------------------------------------------------------------------


  //--------------------PREDEFINED SCHEMA TO READ DATASETS-------------------------
  //Problem: Get min temperatures by Station ID:

  //This is the Structure of the Dataset:
  case class Temperature(stationID: String, date: Int, measure_type: String, temperature: Float)

  //This is the Schema defined
  //Since the csv will not contain any header, inferring schema will not work.
  //The data will be read in this format.
  val temperatureSchema = new StructType()
    .add("stationID", StringType, nullable = true)
    .add("date", IntegerType, nullable = true)
    .add("measure_type", StringType, nullable = true)
    .add("temperature", FloatType, nullable = true)

  // Read the file as dataset

  import spark.implicits._

  val dsTemp = spark.read
    .schema(temperatureSchema)
    .csv("DataFiles/Temperature.csv")
    .as[Temperature]

  //Get only the min temperature columns. We donot need precipitation or max temp data
  val minTemps = dsTemp.filter($"measure_type" === "TMIN")

  //Select only stationID and temperature columns.
  val stationTemps = minTemps.select("stationID", "temperature")

  //Get the min temperature group by StationID
  val minTempsByStation = stationTemps.groupBy("stationID").min("temperature")

  //withColumn()- add new column/change value of an existing column/convert datatype of a column
  //or derive a new column from an existing column for a dataframe/dataset
  val minTempsByStationF = minTempsByStation
    .withColumn("temperature", round($"min(temperature)" * 0.1f * (9.0f / 5.0f) + 32.0f, 2))
    .select("stationID", "temperature").sort("temperature")

  val results = minTempsByStationF.collect()
  for (result <- results) {
    val station = result(0)
    val temp = result(1).asInstanceOf[Float]
    val formattedTemp = f"$temp%.2f F"
    //println(s"$station minimum temperature: $formattedTemp")
  }

  //-------------------------------------------------------------------------------


  //--------------------SPARK INBUILT FUNCTIONS FOR DATASET-----------------------
  //Problem: Get customer bill amounts
  //The Dataset type
  case class CustomerOrders(cust_id: Int, item_id: Int, amount_spent: Double)

  //Read data in this format
  val customerOrdersSchema = new StructType()
    .add("cust_id", IntegerType, nullable = true)
    .add("item_id", IntegerType, nullable = true)
    .add("amount_spent", DoubleType, nullable = true)

  import spark.implicits._

  val customerDS = spark.read
    .schema(customerOrdersSchema)
    .csv("DataFiles/customer-orders.csv")
    .as[CustomerOrders]

  //Explanation:
  //Group by cust_id- Then sum("amount_spent") will calculate the actual sum
  //round(sum("amount_spent"), 2) rounds upto 2dp
  //agg() function -Aggregate the round and sum together in a new column
  //Whose alias will be "total_spent"
  //The agg() method doesn’t perform aggregations but uses functions which do them at the column-level.

  val totalByCustomer = customerDS
    .groupBy("cust_id")
    .agg(round(sum("amount_spent"), 2)
      .alias("total_spent"))

  totalByCustomer.show(totalByCustomer.count.toInt)
}
