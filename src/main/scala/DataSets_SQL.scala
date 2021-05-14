import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object DataSets_SQL extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  //-----While working with we use SparkSession object instead of SparkContext.
  //Once we are done, we Stop the Spark Session using spark.stop()
  val spark = SparkSession
    .builder  //----->>Create the spark session
    .appName("SparkSQL")  //----->>The App Name
    .master("local[*]") //---->>Run on local machine on all CPU
    .getOrCreate()  //---->>Either create a new session or get tyhe existing session of the same name.

  //A case class will be used to hold the schema.
  case class Person(id:Int, name:String, age:Int, friends:Int)

  import spark.implicits._
  val schemaPeople = spark.read
    .option("header", "true") //It has a header row
    .option("inferSchema", "true")  //Try to infer schema from the csv structure.
    .csv("DataFiles/Friends_WithHeader.csv")  //File source
    .as[Person]   //Convert to Dataset(Structure defined in case class)

  //createOrReplaceTempView()-Creates a local temporary view using the given name.
  //The lifetime of this temporary view is tied to the SparkSession that was used to create this Dataset.
  schemaPeople.createOrReplaceTempView("vw_people")

  val teenagers = spark.sql("SELECT * FROM vw_people WHERE age >= 13 AND age <= 19")

  val results = teenagers.collect()
  results.foreach(println)

  //Stop the spark session.
  spark.stop()
}
