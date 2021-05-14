import RDD.{lines, results, sc}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object RDD_KeyValueRDDs extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  //-----Average number of friends by age in a social network-----
  /* The Dataset looks something like:
     (Will ,aged 30, has 385 friends)
      0,Will,30,385
			1,Jean-Luc,25,2
			2,Hugh,30,221
			3,Deanna,25,465
			4,Quark,40,21
   */

  def parseLine(line: String): (Int, Int) = {
    val fields = line.split(",")  //Split by delimitter
    val age = fields(2).toInt //get 2nd field- age
    val numFriends = fields(3).toInt  //get 3rd field-- Num of friends

    (age, numFriends) //Return the Key value Pair tuple
  }

  val sc = new SparkContext("local[*]", "FriendsByAge")
  val lines = sc.textFile("DataFiles/Friends_NoHeader.csv") //Get all the lines of data

  //map method runs the method on all rows of the RDD
  val rdd = lines.map(parseLine)
  //After the above line, we get an RDD as:
  /*
    30,385
	  25,2
	  30,221
	  25,465
	  40,21
   */

  val totalsByAge = rdd.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
  //Note: Here, we have two parts:
  //1st Part. rdd.mapValues(x => (x, 1)), i.e We add a count 1 for each row.
  //mapValues is only applicable for PairRDDs, meaning RDDs of the form RDD[(A, B)]
  //mapValues operates on the value only (the second part of the tuple)
  //After this, we will get an RDD like below:
  // 30,(385,1)
  // 25,(2,1)
  // 30,(221,1)
  // 25,(465,1)
  // 40,(21,1)

  //--2nd Part: Apply reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)) on the above RDD.
  // We get something like:
  //  30,(606,2)	//30,(385+221, 1+1)
  //	25,(467,2)
  //	40,(21,1)

  val averagesByAge = totalsByAge.mapValues(x => x._1 / x._2)
  val results = averagesByAge.collect()
  results.sorted.foreach(println)
  //---------------------------------------------------------------
}
