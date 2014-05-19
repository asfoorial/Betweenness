package uwt.socialnetworks
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import scala.reflect.ClassTag

object Pregel2 extends App 
{
	val sc: SparkContext =
		new SparkContext("local", "SparkPi", System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))

	val nodes: RDD[(VertexId, Int)] = sc.parallelize(Array(
		(1L, 0),
		(2L, 0),
		(3L, 0),
		(4L, 0),
		(5L, 0)))

	// Create an RDD for edges
	val edges: RDD[Edge[Double]] = sc.parallelize(Array(
		Edge(1L, 2L, 5.0),
		Edge(1L, 3L, 1.0),
		Edge(1L, 4L, 4.0),
		Edge(2L, 5L, 3.0),
		Edge(3L, 5L, 4.0),
		Edge(3L, 4L,2.0)))

	val graph = Graph(nodes, edges)	
	// A graph with edge attributes containing distances
	/*val graph: Graph[Int, Double] =
	  GraphGenerators.logNormalGraph(sc, numVertices = 100).mapEdges(e => e.attr.toDouble)*/
	val sourceId: VertexId = 1 // The ultimate source
	// Initialize the graph such that all vertices except the root have distance infinity.
	val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)
	
	myPregel(initialGraph,Double.PositiveInfinity,0)(
	  (id, dist, newDist) =>
	  	{
		  	 //println("Vertex Prog: vertex ID="+id+" vertex val="+dist+" second vertex val="+newDist)
		  	 math.min(dist, newDist)
	  	}, // Vertex Program
	  triplet => {  // Send Message
	  	//println("Message Sender: edge Val="+triplet.attr+" vertex val="+triplet.srcAttr )
	    if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
	    	
	      Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
	    } else {
	      Iterator.empty
	    }
	  },
	  (a,b) => {
	  	//println(a + ",,"+b)
	  	math.min(a,b)} // Merge Message
	  )
    // compute the messages
    /*var messages = g.mapReduceTriplets(sendMsg, mergeMsg)
    var activeMessages = messages.count()*/
	/*val sssp = initialGraph.pregel(Double.PositiveInfinity,0)(
	  (id, dist, newDist) =>
	  	{
		  	 println("Vertex Prog: vertex ID="+id+" vertex val="+dist+" second vertex val="+newDist)
		  	 math.min(dist, newDist)
	  	}, // Vertex Program
	  triplet => {  // Send Message
	  	println("Message Sender: edge Val="+triplet.attr+" vertex val="+triplet.srcAttr )
	    if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
	    	
	      Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
	    } else {
	      Iterator.empty
	    }
	  },
	  (a,b) => {
	  	println(a + ",,"+b)
	  	math.min(a,b)} // Merge Message
	  )
	println(sssp.vertices.collect.mkString("\n"))*/
	 
	 
	 def myPregel[VD: ClassTag, ED: ClassTag, A: ClassTag]
     (graph: Graph[VD, ED],
      initialMsg: A,
      maxIterations: Int = Int.MaxValue,
      activeDirection: EdgeDirection = EdgeDirection.Either)
     (vprog: (VertexId, VD, A) => VD,
      sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
      mergeMsg: (A, A) => A)
    : Graph[VD, ED] =
  {
    var g = graph.mapVertices((vid, vdata) => vprog(vid, vdata, initialMsg)).cache()
    // compute the messages
    var messages = g.mapReduceTriplets(sendMsg, mergeMsg)
    messages.collect.foreach(println(_))
    var activeMessages = messages.count()
    println(activeMessages)
    // Loop
    var prevG: Graph[VD, ED] = null
    var i = 0
    while (activeMessages > 0 && i < maxIterations) {
      // Receive the messages. Vertices that didn't get any messages do not appear in newVerts.
      val newVerts = g.vertices.innerJoin(messages)(vprog).cache()
      // Update the graph with the new vertices.
      prevG = g
      g = g.outerJoinVertices(newVerts) { (vid, old, newOpt) => newOpt.getOrElse(old) }
      g.cache()

      val oldMessages = messages
      // Send new messages. Vertices that didn't get any messages don't appear in newVerts, so don't
      // get to send messages. We must cache messages so it can be materialized on the next line,
      // allowing us to uncache the previous iteration.
      messages = g.mapReduceTriplets(sendMsg, mergeMsg, Some((newVerts, activeDirection))).cache()
      // The call to count() materializes `messages`, `newVerts`, and the vertices of `g`. This
      // hides oldMessages (depended on by newVerts), newVerts (depended on by messages), and the
      // vertices of prevG (depended on by newVerts, oldMessages, and the vertices of g).
      activeMessages = messages.count()
      // Unpersist the RDDs hidden by newly-materialized RDDs
      oldMessages.unpersist(blocking=false)
      newVerts.unpersist(blocking=false)
      prevG.unpersistVertices(blocking=false)
      // count the iteration
      i += 1
    }

    g
  } // end of apply

}