package uwt.socialnetworks
import collection.immutable.HashMap
class VertexData(paths:Int,isVisited1:Boolean,isVisited2:Boolean,lvl:Int) extends Serializable{
	var shortestPathsCount:Int = paths
	var isVisitedOnPass1:Boolean = isVisited1
	var isVisitedOnPass2 = isVisited2
	var messages:HashMap[Int,Message] = new HashMap()
	var message:Message = null
	var credit:Double = 0
	var level:Int = 0
	var levels:scala.collection.mutable.Map[Int,Double] =  scala.collection.mutable.Map[Int,Double]()
	var shortestPaths:scala.collection.mutable.Map[Int,Int] =  scala.collection.mutable.Map[Int,Int]()
	var dlevel:Double = Double.PositiveInfinity
	def this(paths:Int) = this(paths,false,false,0)
	def this(paths:Int,isV:Boolean) = this(paths,isV,false,0)
	def this(paths:Int,isV:Boolean,l:Int) = this(paths,isV,false,l)
	override def toString() = "("+shortestPaths+","+isVisitedOnPass1+","+levels+","+isVisitedOnPass2+","+credit+")"

}