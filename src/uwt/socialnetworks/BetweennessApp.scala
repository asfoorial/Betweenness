package uwt.socialnetworks
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import scala.reflect.ClassTag
import org.apache.velocity.runtime.directive.Foreach
import akka.dispatch.Foreach

object BetweennessApp extends App {
	val sc: SparkContext =
		new SparkContext("local", "SparkPi", System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))

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
	//GraphGenerators.

	
	val root: VertexId = 1

	val initialVertices = nodes.map(v => {
		var vdata: VertexData = null
		if (v._1 == root) {
			vdata = new VertexData(v._2, true)
			(v._1, vdata)
		} else {
			vdata = new VertexData(v._2)
			(v._1, vdata)
		}

	})

	var graph = Graph(initialVertices, edges).cache
	var iterations: Int = initialVertices.count.toInt
	var levelCount = 1
	while (iterations > 1) {
		val newVertices = graph.mapReduceTriplets[VertexData](t => {

			if (t.srcAttr.isVisitedOnPass1 && !t.dstAttr.isVisitedOnPass1) {
				var isVisited = t.dstAttr.isVisitedOnPass1
				var pathCount = t.srcAttr.shortestPathsCount
				if (t.srcId == root)
					pathCount = 1

				var vdata = new VertexData(pathCount, true)
				vdata.level = levelCount
				Iterator((t.dstId, vdata))
			} else
				Iterator.empty
		}, (a, b) => {
			a.shortestPathsCount += b.shortestPathsCount
			a
		})

		iterations = newVertices.count.toInt
		graph = graph.outerJoinVertices(newVertices) { (vid, old, newOpt) => newOpt.getOrElse(old) }
		graph.cache
		levelCount += 1
		println("count is:" + iterations + " and level counter is " + levelCount)
	}

	levelCount -= 1
	graph.vertices.collect.foreach(println(_))
	println("Level is: " + levelCount)

	graph = graph.reverse
	while (levelCount > 0) {
		graph.vertices.collect.foreach(println(_))
		graph = graph.mapVertices((vid, vdata) => {
			if (vdata.level == levelCount)
				vdata.credit += 1
			vdata
		})

		val newVertices = graph.mapReduceTriplets[VertexData](t => {
			println("Dst(" + t.dstId + "[" + t.dstAttr + "]) label:" + t.dstAttr.shortestPathsCount + ", Src (" + t.srcId + "[" + t.srcAttr + "])Label:" + t.srcAttr.shortestPathsCount + " src credit=" + t.srcAttr.credit)

			if (t.srcId != root && t.dstId != root && t.srcAttr.level == levelCount && t.srcAttr.level > t.dstAttr.level) {
				var vdata: VertexData = new VertexData(t.dstAttr.shortestPathsCount, true)
				vdata.credit = t.dstAttr.credit
				//println("Dst("+t.dstId+") label:"+t.dstAttr.shortestPathsCount + ", Src ("+t.srcId+")Label:"+t.srcAttr.shortestPathsCount+ " src credit="+t.srcAttr.credit)
				var propegatedCredit = t.srcAttr.credit
				if (propegatedCredit == 0)
					propegatedCredit = 1
				vdata.credit += (t.dstAttr.shortestPathsCount / (t.srcAttr.shortestPathsCount + 0.0)) * propegatedCredit
				val msg = (t.dstId, vdata)
				vdata.level = t.dstAttr.level
				println("message: " + msg + " dst level=" + t.dstAttr.level)
				Iterator(msg)
			} else
				Iterator.empty
		}, (a, b) => {
			a.credit += b.credit
			a
		})

		graph = graph.outerJoinVertices(newVertices) { (vid, old, newOpt) => newOpt.getOrElse(old) }
		graph.vertices.first
		levelCount -= 1
	}

	graph = graph.mapTriplets(t =>
		{
			println("src(" + t.srcId + ") : " + t.srcAttr + " dst(" + t.dstId + ") : " + t.dstAttr)
			if (t.srcAttr.level > t.dstAttr.level) {
				if (t.dstId == root)
					t.srcAttr.credit / (t.srcAttr.shortestPathsCount + 0.0)
				else
					t.dstAttr.shortestPathsCount / (t.srcAttr.shortestPathsCount + 0.0)
			} else
				t.attr
		})

	graph.edges.collect.foreach(println(_))

	def myPregel[VD: ClassTag, ED: ClassTag, A: ClassTag](graph: Graph[VD, ED],
		initialMsg: A,
		maxIterations: Int = Int.MaxValue,
		activeDirection: EdgeDirection = EdgeDirection.Either)(vprog: (VertexId, VD, A) => VD,
			sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
			mergeMsg: (A, A) => A): Graph[VD, ED] =
		{
			var g = graph.mapVertices((vid, vdata) => vprog(vid, vdata, initialMsg)).cache()
			//g.vertices.collect.foreach(println(_))
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
				oldMessages.unpersist(blocking = false)
				newVerts.unpersist(blocking = false)
				prevG.unpersistVertices(blocking = false)
				// count the iteration
				i += 1
			}

			g
		} // end of apply*/

}
