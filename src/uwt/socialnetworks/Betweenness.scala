package uwt.socialnetworks
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import scala.reflect.ClassTag
import org.apache.velocity.runtime.directive.Foreach
import akka.dispatch.Foreach
import org.apache.spark.graphx.Graph
//Author: Hasan Asfoor

object Betweenness {
	def computeBetweenness(g: Graph[Int, Double], sc:SparkContext) : Graph[VertexData, Double] = {
		
		
	var root: VertexId = 1
	var num:Int = (g.numVertices*0.1).toInt
	var graph:Graph[VertexData,Double] = null
	num=1
	
	graph = g.mapVertices((vId,value) => {
			var vdata: VertexData = null
			if (vId == root) {
				vdata = new VertexData(value, true)
				vdata
			} else {
				vdata = new VertexData(value)
				vdata
			}
	
		})
	
	/*val initialVertices = g.vertices.map(v => {
			var vdata: VertexData = null
			if (v._1 == root) {
				vdata = new VertexData(v._2, true)
				(v._1, vdata)
			} else {
				vdata = new VertexData(v._2)
				(v._1, vdata)
			}
	
		})*/
	for(vId<- 1 to num)
	{
		root = vId

		var iterations: Int = 2 //just an initial value. it will change inside the loop
		var levelCount = 1
		
		var newVertices = graph.mapReduceTriplets[VertexData](t => {
	
				if (t.srcAttr.isVisitedOnPass1 && !t.dstAttr.isVisitedOnPass1) {
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
		while (iterations > 1) {
			
	
			
			graph = graph.outerJoinVertices(newVertices) { (vid, old, newOpt) => newOpt.getOrElse(old) }
			graph.cache
			levelCount += 1
			
			newVertices = graph.mapReduceTriplets[VertexData](t => {
	
				if (t.srcAttr.isVisitedOnPass1 && !t.dstAttr.isVisitedOnPass1) {
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
			},
			Some((newVertices, EdgeDirection.Out))
			)
			
		iterations = newVertices.count.toInt
			//println("count is:" + iterations + " and level counter is " + levelCount)
		}
	
		levelCount -= 1
		//graph.vertices.collect.foreach(println(_))
		//println("Level is: " + levelCount)
	
		graph = graph.reverse
		
		graph = graph.mapVertices((vid, vdata) => {
				if (vdata.level == levelCount)
					vdata.credit += 1
				vdata
			})
	
			newVertices = graph.mapReduceTriplets[VertexData](t => {
				//println("Dst(" + t.dstId + "[" + t.dstAttr + "]) label:" + t.dstAttr.shortestPathsCount + ", Src (" + t.srcId + "[" + t.srcAttr + "])Label:" + t.srcAttr.shortestPathsCount + " src credit=" + t.srcAttr.credit)
	
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
					//println("message: " + msg + " dst level=" + t.dstAttr.level)
					Iterator(msg)
				} else
					Iterator.empty
			}, (a, b) => {
				a.credit += b.credit
				a
			})
			
		while (levelCount > 0) {
			//graph.vertices.collect.foreach(println(_))
			
	
			graph = graph.outerJoinVertices(newVertices) { (vid, old, newOpt) => newOpt.getOrElse(old) }
			graph.vertices.first
			levelCount -= 1
			
			graph = graph.mapVertices((vid, vdata) => {
				if (vdata.level == levelCount)
					vdata.credit += 1
				vdata
			})
	
			newVertices = graph.mapReduceTriplets[VertexData](t => {
				//println("Dst(" + t.dstId + "[" + t.dstAttr + "]) label:" + t.dstAttr.shortestPathsCount + ", Src (" + t.srcId + "[" + t.srcAttr + "])Label:" + t.srcAttr.shortestPathsCount + " src credit=" + t.srcAttr.credit)
	
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
					//println("message: " + msg + " dst level=" + t.dstAttr.level)
					Iterator(msg)
				} else
					Iterator.empty
			}, (a, b) => {
				a.credit += b.credit
				a
			},
			Some((newVertices, EdgeDirection.Out))
			)
			
		}
	
		graph = graph.mapTriplets(t =>
			{
				//println("src(" + t.srcId + ") : " + t.srcAttr + " dst(" + t.dstId + ") : " + t.dstAttr)
				if (t.srcAttr.level > t.dstAttr.level) {
					if (t.dstId == root)
						t.srcAttr.credit / (t.srcAttr.shortestPathsCount + 0.0)
					else
						t.dstAttr.shortestPathsCount / (t.srcAttr.shortestPathsCount + 0.0)
				} else
					t.attr
			})
	
		//graph.edges.collect.foreach(println(_))
	
		}
		return graph
	}
	
	
	
	
	def computeBetweenness2(g: Graph[Int, Double], sc:SparkContext) : Graph[VertexData, Double] = {
		
		
	var root: VertexId = 1
	var num:Int = (g.numVertices*0.1).toInt
	var graph:Graph[VertexData,Double] = null
	num=1
	for(vId<- 1 to num)
	{
		root = vId
		val initialVertices = g.vertices.map(v => {
			var vdata: VertexData = null
			if (v._1 == root) {
				vdata = new VertexData(v._2, true)
				(v._1, vdata)
			} else {
				vdata = new VertexData(v._2)
				(v._1, vdata)
			}
	
		})
	
		graph = Graph(initialVertices, g.edges).cache
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
			//println("count is:" + iterations + " and level counter is " + levelCount)
		}
	
		levelCount -= 1
		//graph.vertices.collect.foreach(println(_))
		//println("Level is: " + levelCount)
	
		graph = graph.reverse
		while (levelCount > 0) {
			//graph.vertices.collect.foreach(println(_))
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
					//println("message: " + msg + " dst level=" + t.dstAttr.level)
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
				//println("src(" + t.srcId + ") : " + t.srcAttr + " dst(" + t.dstId + ") : " + t.dstAttr)
				if (t.srcAttr.level > t.dstAttr.level) {
					if (t.dstId == root)
						t.srcAttr.credit / (t.srcAttr.shortestPathsCount + 0.0)
					else
						t.dstAttr.shortestPathsCount / (t.srcAttr.shortestPathsCount + 0.0)
				} else
					t.attr
			})
	
		//graph.edges.collect.foreach(println(_))
	
		}
		return graph
	}
	

}