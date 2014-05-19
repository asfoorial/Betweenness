package uwt.socialnetworks

import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.VertexRDD
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object PregelTest extends App {
	val sc: SparkContext =
		new SparkContext("local", "SparkPi", System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))

	val nodes: RDD[(VertexId, (String, Int))] = sc.parallelize(Array(
		(1L, ("A", 0)),
		(2L, ("B", 0)),
		(3L, ("C", 0)),
		(4L, ("D", 0)),
		(5L, ("E", 0)),
		(6L, ("F", 0)),
		(7L, ("G", 0))))

	// Create an RDD for edges
	val edges: RDD[Edge[String]] = sc.parallelize(Array(
		//Edge(5L, 4L, "E-D"),
		Edge(4L, 5L, "D-E"),
		//Edge(5L, 6L, "E-F"),
		Edge(6L, 5L, "F-E"),
		//Edge(4L, 6L, "D-F"),
		Edge(6L, 4L, "F-D"),
		//Edge(4L, 7L, "D-G"),
		Edge(7L, 4L, "G-D"),
		//Edge(7L, 6L, "G-F"),
		Edge(6L, 7L, "F-G"),
		//Edge(4L, 2L, "D-B"),
		Edge(2L, 4L, "B-D"),
		//Edge(2L, 1L, "B-A"),
		Edge(1L, 2L, "A-B"),
		//Edge(2L, 3L, "B-C"),
		Edge(3L, 2L, "C-B"),
		//Edge(1L, 3L, "A-C"),
		Edge(3L, 1L, "C-A")))

	val graph = Graph(nodes, edges)
	
	graph.triplets.map(t=>{
		if(t.srcAttr._1.equals("A"))
			t.srcAttr = (t.srcAttr._1,2)
		t.srcAttr + ":" + t.dstAttr + ":"+ t.attr}).collect.foreach(println(_))

	/*val pagerankGraph: Graph[Double, Double] = graph
		// Associate the degree with each vertex
		.outerJoinVertices(graph.outDegrees) {
			(vid, vdata, deg) => deg.getOrElse(0)
		}
		// Set the weight on the edges based on the degree
		.mapTriplets(e => 1.0 / e.srcAttr)
		// Set the vertex attributes to the initial pagerank values
		.mapVertices((id, attr) => 1.0)

	var resetProb = 5;

	def vertexProgram(id: VertexId, attr: Double, msgSum: Double): Double =
		resetProb + (1.0 - resetProb) * msgSum
	def sendMessage(id: VertexId, edge: EdgeTriplet[Double, Double]): Iterator[(VertexId, Double)] =
		Iterator((edge.dstId, edge.srcAttr * edge.attr))
	def messageCombiner(a: Double, b: Double): Double = a + b
	val initialMessage = 0.0
	// Execute Pregel for a fixed number of iterations.
	Pregel(pagerankGraph, initialMessage, 5)(
		vertexProgram, sendMessage, messageCombiner)*/

		
		
		
	val sourceId: VertexId = 1 // The ultimate source
	// Initialize the graph such that all vertices except the root have distance infinity.
	/*val initialGraph = graph.mapVertices((id, value) => if (id == sourceId) (value._1,0) else (value._1,-1))
	//initialGraph.vertices.collect.foreach(println(_))
	val sssp = initialGraph.pregel(3,2)(
		(id, dist, newDist) => 
			{
				println(id + ":" +dist + ":" +newDist)
				(dist._1,dist._2+1)}, // Vertex Program
		triplet => { // Send Message
			if(triplet.srcAttr._2>=0)
			Iterator((triplet.dstId, triplet.srcAttr._2))
			else
				Iterator.empty
		},
		(a, b) => a+b// Merge Message
		)
	println(sssp.vertices.collect.mkString("\n"))*/
	

}