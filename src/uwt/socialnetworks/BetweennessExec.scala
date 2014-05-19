package uwt.socialnetworks
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import scala.reflect.ClassTag
import org.apache.velocity.runtime.directive.Foreach
import akka.dispatch.Foreach
import java.io._
object BetweennessExec extends App {

	val sc: SparkContext = new SparkContext("spark://master:7077", "SparkPi", "/home/spark/spark0.9", Array("/home/spark/apps/betweenness/betweenness.jar"))
	//val sc: SparkContext = new SparkContext("local", "SparkPi", "/home/spark/spark0.9", Array("/home/spark/apps/betweenness/betweenness.jar"))
	var grf = getSimpleGraph

	var rg = Betweenness.computeBetweenness(grf, sc)
	
	rg.edges.collect.foreach(println(_))
	
	def getGraphFromFile(fileName:String, sc:SparkContext): Graph[Int,Double] = 
	{
		var g = GraphLoader.edgeListFile(sc, "/home/spark/apps/graphx/edges.txt", true, 7)
		var edges = g.edges.mapValues(v=>0.0)
		var vertices = g.vertices.mapValues(v=>v.toInt)
		var grf = Graph(vertices,edges)
		return grf
	}
	
	def generateGraph(numOfVertices:Int, numOfEdgesPerVertex:Int, location:String) =
	{
		val writer = new PrintWriter(new File(location))
		for(i<- 1 to numOfVertices)
		{
			var e = GraphGenerators.generateRandomEdges(i, numOfEdgesPerVertex, numOfVertices)
			
			e.foreach(e=>{
				writer.write(e.srcId + " " +e.dstId + "\n")
			})
		}
	      
		writer.close()
	}
	def getSimpleGraph(): Graph[Int,Double] = 
	{
		val nodes: RDD[(VertexId, Int)] = sc.parallelize(Array(
		(1L, 0),
		(2L, 0),
		(3L, 0),
		(4L, 0),
		(6L, 0),
		(7L, 0),
		(8L, 0),
		(5L, 0)))

	// Create an RDD for edges
	val edges: RDD[Edge[Double]] = sc.parallelize(Array(
		Edge(1L, 2L, 0.0),
		Edge(1L, 2L, 0.0),
		Edge(2L, 3L, 0.0),
		Edge(1L, 3L, 0.0),
		Edge(2L, 4L, 0.0),
		Edge(3L, 4L, 0.0),
		Edge(2L, 5L, 0.0),
		Edge(5L, 6L, 0.0),
		Edge(1L, 8L, 0.0),
		Edge(8L, 4L, 0.0),
		Edge(5L, 7L, 0.0)))
		
	var graph = Graph(nodes, edges)
	return graph
	}
}