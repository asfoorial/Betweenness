package uwt.socialnetworks
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.VertexId
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.EdgeTriplet
import org.apache.spark.graphx.EdgeDirection
import org.apache.commons.lang.mutable.Mutable
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.graphx.VertexRDD

import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import scala.reflect.ClassTag
import org.apache.velocity.runtime.directive.Foreach
import akka.dispatch.Foreach

object BetweennessPregel extends App {

	val sc: SparkContext =
		new SparkContext("local", "Betweenness", System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))
	//testPregel
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
		Edge(5L, 7L, 0.0) /*,
		
		Edge(2L, 1L, 0.0),
		Edge(2L, 1L, 0.0),
		Edge(3L, 2L, 0.0),
		Edge(3L, 1L, 0.0),
		Edge(4L, 2L, 0.0),
		Edge(4L, 3L, 0.0),
		Edge(5L, 2L, 0.0),
		Edge(6L, 5L, 0.0),
		Edge(8L, 1L, 0.0),
		Edge(4L, 8L, 0.0),
		Edge(7L, 5L, 0.0)*/ ))

	val initialVertices = nodes.map(v => {
		var vdata: VertexData = null
		if (v._1 == 1) {
			vdata = new VertexData(v._2, true)
			vdata.dlevel = 0
			(v._1, vdata)
		}
		else {
			vdata = new VertexData(v._2)
			vdata.dlevel = Double.PositiveInfinity
			(v._1, vdata)
		}

	})

	var graph = Graph(initialVertices, edges).cache
	var root = 1
	var msg = new Message(-1, 1, Double.PositiveInfinity, 0, EdgeDirection.Out)
	msg.roots = List(1)
	graph = graph.pregel(msg)(
		(vid, vertex, message) => {
			println("vid=" + vid + " node=" + vertex + "; received msg: " + message)
			var vdata = new VertexData(0, true)
			message.messages.foreach(msg => {
				if (msg.messageId == -1) {
					msg.roots.foreach(i => {
						vdata.levels += (i -> Double.PositiveInfinity)
						vdata.shortestPaths += (i -> 0)
					})
					if (msg.roots.contains(vid.toInt))
						vdata.levels(vid.toInt) = 0.0
				}
				else {
					vdata.levels = vertex.levels
					vdata.levels(msg.messageId) = msg.level
					//println("msgid="+msg.messageId+"; msg.shortestPathCount="+msg.shortestPathCount+"; vdata.shortestPaths="+vdata.shortestPaths)
					vdata.shortestPaths = vertex.shortestPaths
					vdata.shortestPaths(msg.messageId) += msg.shortestPathCount
				}
			})

			vdata

		}, // Vertex Program
		triplet => { // Send Message 
			//println("triplets:" + triplet)
			var resultList: List[(VertexId, Message)] = List[(VertexId, Message)]()
			var roots = triplet.srcAttr.levels;
			if (roots.size <= 0) {
				roots = triplet.dstAttr.levels;
			}
			if (roots.size > 0) {

				//println("roots exist")

				roots.keys.foreach(i => {
					var src = triplet.srcAttr
					var dst = triplet.dstAttr
					if (src.levels(i) > dst.levels(i)) {
						src = triplet.dstAttr
						dst = triplet.srcAttr
					}
					var srcLevel = triplet.srcAttr.levels.getOrElse(i, Double.PositiveInfinity)
					var dstLevel = triplet.dstAttr.levels.getOrElse(i, Double.PositiveInfinity)
					var srcId = triplet.srcId
					var dstId = triplet.dstId
					var srcPathesCount = triplet.srcAttr.shortestPaths.getOrElse(i, 0)
					//println("srcLevel="+srcLevel+"; dstLevel="+dstLevel+"; root="+i)
					if (srcLevel != dstLevel && (srcLevel.isPosInfinity || dstLevel.isPosInfinity)) {
						if (srcLevel > dstLevel) {
							var templvl = srcLevel
							srcLevel = dstLevel
							dstLevel = templvl

							var tempId = srcId
							srcId = dstId
							dstId = tempId
							srcPathesCount = triplet.dstAttr.shortestPaths.getOrElse(i, 0)
						}
						var pathcount = srcPathesCount
						if (pathcount == 0)
							pathcount = 1
						var msg = new Message(i, dstId.toInt, srcLevel + 1, pathcount, EdgeDirection.Out)
						println("sending msg from src=" + srcId + " (lvl=" + srcLevel + " to dstId=" + dstId + " (lvl=" + dstLevel + "; msg=" + msg)
						resultList ::= (dstId, msg)
					}
				})

			}

			resultList.iterator

		},
		(a, b) =>
			{
				a.messages ::= b
				a
			} // Merge Message
			)
	println(graph.vertices.collect.mkString("\n"))
	//Done with pass 1
	/*
	 * Now setup edges to handle a credit for every root. The credit is set to initial infinity!
	 */
	var newGraph = graph.mapEdges(e => {
		var credits = scala.collection.mutable.Map[Int, Double]()
		msg.roots.foreach(i => {
			credits += (i -> Double.PositiveInfinity)
		})
	})
	
	/*
	 * Maximum levels at each root
	 */
	var maxLevels = graph.vertices.map(v => v._2.levels).reduce((l1, l2) => {
		var l3 = scala.collection.mutable.Map[Int, Double]()
		l1.keys.foreach(i => {
			l3 += (i -> Math.max(l1(i), l2(i)))
		})
		l3
	})

	var newVs = newGraph.triplets.map(t=>{
		/*var roots = t.srcAttr.levels;
		if (roots.size <= 0) {
			roots = t.dstAttr.levels;
		}
		if (roots.size > 0) {

			//println("roots exist")

			roots.keys.foreach(i => {
				var src = t.srcAttr
				var dst = t.dstAttr
				var srcId = t.srcId
				var dstId = t.dstId
				if (src.levels(i) < dst.levels(i)) {
					src = t.dstAttr
					dst = t.srcAttr

					dstId = t.srcId
					srcId = t.dstId
				}
				print("pass")
				
			})
		}
		triplet.srcAttr.isLeaf = false
		triplet.srcAttr*/
		t.srcAttr.isLeaf = false
		(t.srcId,t.srcAttr)
	})
	
	/*
	 * Modify this part. And add the map for the isLeaf and handle it in the rest of the code. Make sure to set isLeaf to false properly
	 */
	var newVs2 = newGraph.triplets.map(t=>{
		t.dstAttr.isLeaf = false
		(t.dstId,t.dstAttr)
	})
	newVs = newVs.union(newVs2)
	newVs = newVs.distinct
	newGraph = Graph(newVs, newGraph.edges).cache
	
	//newGraph.mapReduceTriplets(mapFunc, reduceFunc, activeSetOpt)
	println("new graph\n" + newGraph.vertices.collect.mkString("\n"))
	println("maxLevels\n" + maxLevels.mkString("\n"))
	/*newGraph = newGraph.pregel(msg)(
		(vid, vertex, message) => {
			println("vid=" + vid + " node=" + vertex + "; received msg: " + message + "; maxLevels=" + maxLevels)
			var vdata = new VertexData(0, true)
			vdata.levels = vertex.levels
			vdata.shortestPaths = vertex.shortestPaths
			vdata.credits = vertex.credits
			message.messages.foreach(msg => {
				if (msg.messageId == -1) {
					msg.roots.foreach(i => {
						var creditVal = Double.PositiveInfinity
						if (vdata.levels(i) == maxLevels(i))
							creditVal = 1
						vdata.credits += (i -> creditVal)
						vdata.levels = vertex.levels
						vdata.shortestPaths = vertex.shortestPaths

					})
				}
				else {
					//println("msgid="+msg.messageId+"; msg.shortestPathCount="+msg.shortestPathCount+"; vdata.shortestPaths="+vdata.shortestPaths)
					vdata.credits(msg.messageId) = msg.credit
				}
			})

			vdata

		}, // Vertex Program
		triplet => { // Send Message 
			//println("triplets:" + triplet)
			var resultList: List[(VertexId, Message)] = List[(VertexId, Message)]()
			var roots = triplet.srcAttr.levels;
			if (roots.size <= 0) {
				roots = triplet.dstAttr.levels;
			}
			if (roots.size > 0) {

				//println("roots exist")

				roots.keys.foreach(i => {
					var src = triplet.srcAttr
					var dst = triplet.dstAttr
					var srcId = triplet.srcId
					var dstId = triplet.dstId
					if (src.levels(i) < dst.levels(i)) {
						src = triplet.dstAttr
						dst = triplet.srcAttr

						dstId = triplet.srcId
						srcId = triplet.dstId
					}
					var srcLevel = src.levels.getOrElse(i, Double.PositiveInfinity)
					var dstLevel = dst.levels.getOrElse(i, Double.PositiveInfinity)

					var srcPathesCount = src.shortestPaths.getOrElse(i, 0)
					var srcCredit = src.credits(i)
					var dstCredit = dst.credits(i)

					//println("srcLevel="+srcLevel+"; dstLevel="+dstLevel+"; root="+i)
					if (srcCredit != dstCredit && dstCredit.isPosInfinity) {

						var msg = new Message(i, dstId.toInt, -1, -1, EdgeDirection.Out)
						msg.credit = srcCredit + 1
						println("sending msg from src=" + srcId + " (lvl=" + srcLevel + " to dstId=" + dstId + " (lvl=" + dstLevel + "; msg=" + msg)
						resultList ::= (dstId, msg)
					}
				})

			}
			resultList.iterator

		},
		(a, b) =>
			{
				a.messages ::= b
				a
			} // Merge Message
			)
	//graph.reverse
	println(newGraph.vertices.collect.mkString("\n"))*/

	def myPregel[VD: ClassTag, ED: ClassTag, A: ClassTag](grf: Graph[VD, ED],
		initialMsg: A,
		maxIterations: Int = Int.MaxValue,
		activeDirection: EdgeDirection = EdgeDirection.Either)(vprog: (VertexId, VD, A) => VD,
			sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
			mergeMsg: (A, A) => A): Graph[VD, ED] =
		{
			println("Executing vprog...")
			var g = grf.mapVertices((vid, vdata) => vprog(vid, vdata, initialMsg)).cache()
			//g.vertices.collect.foreach(println(_))
			println("vertices after executing vprog: " + g.vertices.collect.mkString("\n"))
			// compute the messages
			var messages = g.mapReduceTriplets(sendMsg, mergeMsg)
			println("new msgs: " + messages.collect.mkString("\n"))

			var activeMessages = messages.count()
			println(activeMessages)
			// Loop
			var prevG: Graph[VD, ED] = null
			var i = 0
			while (activeMessages > 0 && i < maxIterations) {
				// Receive the messages. Vertices that didn't get any messages do not appear in newVerts.
				println("Executing vprog...")
				val newVerts = g.vertices.innerJoin(messages)(vprog).cache()

				// Update the graph with the new vertices.
				prevG = g
				g = g.outerJoinVertices(newVerts) { (vid, old, newOpt) => newOpt.getOrElse(old) }

				println("vertices after executing vprog: " + g.vertices.collect.mkString("\n"))
				g.cache()
				val oldMessages = messages
				// Send new messages. Vertices that didn't get any messages don't appear in newVerts, so don't
				// get to send messages. We must cache messages so it can be materialized on the next line,
				// allowing us to uncache the previous iteration.
				g.triplets.map(t => {
					println("checking triplets!: " + t)
					t
				}).first
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
		}

	def updateLevels() {
		val sc: SparkContext =
			new SparkContext("local", "Betweenness", System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))
		//testPregel
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

		val initialVertices = nodes.map(v => {
			var vdata: VertexData = null
			if (v._1 == 1) {
				vdata = new VertexData(v._2, true)
				vdata.dlevel = 0
				(v._1, vdata)
			}
			else {
				vdata = new VertexData(v._2)
				vdata.dlevel = Double.PositiveInfinity
				(v._1, vdata)
			}

		})

		/*val initialVertices = nodes.map(v => {
		if (v._1 == 1) {
			(v._1, 0.0)
		} else {
			(v._1, Double.PositiveInfinity)
		}

	})*/

		var graph = Graph(initialVertices, edges).cache

		var root = 1
		var msg = new Message(1, 1, Double.PositiveInfinity, 0, EdgeDirection.Out)
		val sssp = graph.pregel(msg)(
			(vid, vertex, msg) => {
				println("received msg: " + msg)
				var vdata = new VertexData(0, true)
				if (vid == root)
					vdata.dlevel = 0
				else
					vdata.dlevel = msg.level
				vdata.message = msg
				vdata

			}, // Vertex Program
			triplet => { // Send Message 
				println("triplets:" + triplet)
				println("srcId=" + triplet.srcId + "; message=" + triplet.srcAttr.message)
				if (triplet.srcAttr.dlevel < triplet.dstAttr.dlevel && triplet.dstAttr.dlevel.isPosInfinity) {
					var msg = new Message(triplet.srcId.toInt, triplet.dstId.toInt, triplet.srcAttr.dlevel + 1, 1, EdgeDirection.Out)
					Iterator((triplet.dstId, msg))
				}
				else
					Iterator.empty

			},
			(a, b) =>
				{

					a
				} // Merge Message
				)
		println(sssp.vertices.collect.mkString("\n"))
	}
}