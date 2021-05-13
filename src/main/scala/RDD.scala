import org.apache.spark.SparkContext
import org.apache.log4j._

object RDD extends App {

  //----For Setting Log Level-----
  Logger.getLogger("org").setLevel(Level.ERROR)

  //----Create Spark Context-----
  val sc = new SparkContext("local[*]", "RatingsCounter")

  //-----Use the Spark Context to get textfile
  val lines = sc.textFile("DataFiles/movieRatings.data")
  val ratings = lines.map(x => x.split("\t")(2))
  val results = ratings.countByValue()
  val sortedResults = results.toSeq.sortBy(x => x._1)
  sortedResults.foreach(println)
}
