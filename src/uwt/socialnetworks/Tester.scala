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
object Tester extends App {

	val sc: SparkContext =
		new SparkContext("local", "Betweenness", System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))

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
	var triplets = graph.triplets.filter(t => t.srcId == root)
	//println(triplets.count)
	var vertices = triplets.map(t =>
		{
			t.dstAttr.level = 1
			t.dstAttr.shortestPathsCount = 1
			t.dstAttr.isVisitedOnPass1 = true
			(t.dstId, t.dstAttr)
		})

	var currentLevel: Int = 1
	graph = graph.outerJoinVertices(vertices)((vid, old, newOpt) => newOpt.getOrElse(old))
	var verticesToVisit = vertices.count.toInt

	while (verticesToVisit > 0) {
		triplets = graph.triplets.filter(t => {
			//println("triplets: "+t)
			t.srcAttr.level == currentLevel && t.dstAttr.level != currentLevel
		})
		//println("level:" + currentLevel + " triplet count=" + triplets.count)

		vertices = triplets.map(t =>
			{
				if (!t.dstAttr.isVisitedOnPass1)
					t.dstAttr.level = t.srcAttr.level + 1
				t.dstAttr.shortestPathsCount += 1
				t.dstAttr.isVisitedOnPass1 = true
				//println("triplets to vertices: " + t)
				(t.dstId, t.dstAttr)
			})
		verticesToVisit = vertices.count.toInt

		//println("level:"+currentLevel+" vertices count="+verticesToVisit)

		//vertices.collect.foreach(println(_))
		graph = graph.outerJoinVertices(vertices)((vid, old, newOpt) =>
			{
				//println("vid=" + vid + ", old=" + old + ", newOpt=" + newOpt)
				newOpt.getOrElse(old)

			})
		graph.vertices.first

		currentLevel += 1

	}

	currentLevel -= 1
	graph.vertices.collect.foreach(println(_))

	println(currentLevel)
	triplets = graph.triplets.filter(t => t.dstAttr.level == currentLevel && t.dstAttr.level > t.srcAttr.level)
	println(triplets.count)
	vertices = triplets.map(t =>
		{
			t.dstAttr.credit = 1
			println("triplets to vertices: " + t)
			//t.dstAttr.credit += (t.dstAttr.shortestPathsCount / (t.srcAttr.shortestPathsCount + 0.0)) * t.srcAttr.credit
			(t.dstId, t.dstAttr)
		})
	verticesToVisit = vertices.count.toInt
	//currentLevel -= 1
	vertices.collect.foreach(println(_))
	graph = graph.outerJoinVertices(vertices)((vid, old, newOpt) => newOpt.getOrElse(old))
	
	while (currentLevel > 0) {

		triplets = graph.triplets.filter(t => t.dstAttr.level == currentLevel && t.dstAttr.level > t.srcAttr.level)
		//println("level:" + currentLevel + " triplet count=" + triplets.count)
		vertices = triplets.map(t =>
			{
				println("level="+currentLevel+"; triplets to vertices: " + t)
				t.srcAttr.credit += (t.srcAttr.shortestPathsCount / (t.dstAttr.shortestPathsCount + 0.0)) * t.dstAttr.credit
				println("level="+currentLevel+"; triplets to vertices: " + t)
				(t.srcId, t.srcAttr)
			})
		verticesToVisit = vertices.count.toInt

		//println("level:"+currentLevel+" vertices count="+verticesToVisit)

		//vertices.collect.foreach(println(_))
		graph = graph.outerJoinVertices(vertices)((vid, old, newOpt) =>
			{
				//println("vid=" + vid + ", old=" + old + ", newOpt=" + newOpt)
				newOpt.getOrElse(old)

			})
		graph.vertices.first

		currentLevel -= 1

	}

	graph.vertices.collect.foreach(println(_))

}
