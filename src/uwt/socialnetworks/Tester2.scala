package uwt.socialnetworks

import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.VertexRDD
import org.apache.spark.SparkContext
import  scala.collection.mutable._
object Tester2 extends App {

	var map = scala.collection.mutable.Map[Int,Int]()
	map+=(1->2)
	map(1) = 9
}