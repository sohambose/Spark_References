import org.apache.spark.SparkContext
import org.apache.log4j._

object RDD extends App {

  //----For Setting Log Level-----
  Logger.getLogger("org").setLevel(Level.ERROR)

  //-------- Spark Context ----------------------------------------
  //local[*] -- Local means local machine(We do not have a cluster of machines)
  //-> [*]- means Parallelize within the local multiple CPU cores.
  //->RatingsCounter is the name of the application
  val sc = new SparkContext("local[*]", "RatingsCounter")
  //------------------------------------------------------------

  //-------- Simple RDD using Small Static Data -----------------
  val rdd1 = sc.parallelize(List(1, 2, 3, 4))
  val sqrLst = rdd1.map(x => x * x) //sqrLst= 1,4,9,16
  //sqrLst.foreach(println)
  //------------------------------------------------------------


  //----------Find out which rating got the most count------------
  //Read from text File:
  val lines = sc.textFile("DataFiles/movieRatings.data")

  //Extract the ratings column only
  //This will contain one column with ratings 1,2,3,4 only
  val ratings = lines.map(x => x.split("\t")(2))

  //CountByValue() will return a map. (Value, Count). E.g: (4,34174)
  val results = ratings.countByValue()

  //Sort based on 2nd column.i.e the count column
  val sortedResults = results.toSeq.sortBy(x => x._2)
  //sortedResults.foreach(println)
  //------------------------------------------------------------
}
