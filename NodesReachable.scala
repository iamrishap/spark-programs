package comp9313.ass3

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx._

/*
* @Author: Rishap Sharma (UNSW 18s1: z5202535)
* Pregel GraphX Scala API based implementation to find the number of nodes 
* reacable from a particular node in a Graph. 
* It uses Single Source Shortest Path to find the shortest paths to various nodes.
* Any node which has distance infinity(Double max) after the iterations is unreachable. 
* Input is 1. file with each lines describing graph edges and vertices 2. Source Node
* Output is integer value describing the number of nodes that are reachable
* 
* Input file format : serial_no<tab>source_node<tab>target_node<tab>distance
* Output format : <Int>nodesReachable
* 
*/

object Problem2 {
  def main(args: Array[String]) {
    val inputFile = args(0)
    val sourceId = args(1).toInt  // Source Node to find the nodes reachable
    val conf = new SparkConf().setAppName("NodesReachable").setMaster("local")
    val sc = new SparkContext(conf)
    val edges = sc.textFile(inputFile)
    val edgelist = edges.map(x => x.split(" ")).map(x=> Edge(x(1).toLong, x(2).toLong, 1.toDouble))  // As the weight value is irrelevant, we just choose to keep value 1 as the connection.
    val graph = Graph.fromEdges[Double, Double](edgelist, 1)  // Create Graph using the EdgesTuples. By default the weight is 1.
    val graphInitially = graph. mapVertices((id,_) => if (id.toString() == sourceId.toString) 0.0 else Double.PositiveInfinity)  // Source distance 0 others unreachable for now.
    
    // Pregel operation to calculate the hops required to reach various nodes from the source node
    val sourceSteps = graphInitially.pregel(Double.PositiveInfinity)(
      (id, hops, newHops) => math.min(hops, newHops),  // Vertex receiver
      triplet => { // Message Passer
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (x,y) => x  // Merge Step
    )
    val nodesReachable = sourceSteps.vertices.filter(t => t._2 != Double.PositiveInfinity)  // Filter out vertices for which the hops is infinity.
    println(nodesReachable.count() - 1)  // Final count.
  }
}
