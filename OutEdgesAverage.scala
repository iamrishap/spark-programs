package comp9313.ass3
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions


/*
* @Author: Rishap Sharma (UNSW 18s1: z5202535)
* Scala based implementation to find the Object for finding the Average Length of the Outgoing edges in the graph.
* 
* Input file format : serial_no<tab>source_node<tab>target_node<tab>distance
* Output format : nodeId<tab>nodeOutgoingEdgesAvg
* 
*/
// 
object Problem1 {
  def main(args: Array[String]) {
    val inputFile = args(0)  // Input file
    val outputFolder = args(1)  // Output folder path
    val conf = new SparkConf().setAppName("OutEdgesAvg").setMaster("local") // Configuration
    val sc = new SparkContext(conf) // Spark Context setup
    val input = sc.textFile(inputFile)  // Reading the input
    val edgeMap = input.map(x => x.split(" ")).map(x=> (x(1).toLong, (x(3).toDouble, 1)))  // x(2).toLong (Destination) is irrelevant for this question
    val lenAvg = edgeMap.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2)).map(x=> (x._1, x._2._1.toDouble/x._2._2))  // Reducing by Key (addding the count and sum) and then finding the average
    val sortedLenAvg = lenAvg.sortByKey(true, 1).sortBy(_._2, false)  // First sort Key based, second sort value based. Gives the desired result format.
    val formattedLenAvg = sortedLenAvg.filter(_._2 > (0).toDouble).map(x=>s"${x._1}\t${x._2}")  // Output formatting
    formattedLenAvg.saveAsTextFile(outputFolder)  // Writing the output
   }
}