package uwt.socialnetworks

import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.VertexRDD
import org.apache.spark.SparkContext
import org.apache.spark.graphx._

object TestTriplets extends App {

	val sc: SparkContext =
		new SparkContext("local", "SparkPi", System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))

	val graph: Graph[Int, Double] =
		GraphGenerators.logNormalGraph(sc, numVertices = 100).mapEdges(e => e.attr.toDouble)
		
	//graph.edges.collect.foreach(println(_))
	val sourceId: VertexId = 42 // The ultimate source
	// Initialize the graph such that all vertices except the root have distance infinity.
	val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)
	val sssp = initialGraph.pregel(Double.PositiveInfinity)(
		(id, dist, newDist) => 
			{
				println(id+":"+dist+":"+newDist)
			math.min(dist, newDist)}, // Vertex Program
		
		triplet => { // Send Message
			if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
				Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
			} else {
				Iterator.empty
			}
		},
		(a, b) => math.min(a, b) // Merge Message
		)
	println(sssp.vertices.collect.mkString("\n"))
	
	
	/*val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)

	val bfs = initialGraph.pregel(Double.PositiveInfinity)((id, attr, msg) => math.min(attr, msg), triplet => { if (triplet.srcAttr != Double.PositiveInfinity && triplet.dstAttr == Double.PositiveInfinity ) { Iterator((triplet.dstId, triplet.srcAttr + 1)) } else { Iterator.empty } }, (a, b) => math.min(a, b))

	println(bfs.edges.collect.mkString("\n"))*/

}