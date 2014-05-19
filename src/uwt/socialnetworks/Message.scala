package uwt.socialnetworks

import org.apache.spark.graphx.EdgeDirection

class Message (msgId:Int, msgDst:Int, lvl:Double, pathcount:Int, dir:EdgeDirection) extends Serializable{
	var direction:EdgeDirection = dir
	var messageId:Int = msgId
	var messages:List[Message] = List[Message](this)
	var dstId:Int = msgDst
	var level:Double = lvl
	var credit:Double = 0
	var shortestPathCount:Int = pathcount
	var isCancel:Boolean = false
	var roots:List[Int] = null
	override def toString() = "(msgId="+messageId+", dst="+msgDst+",lvl="+level+",paths="+shortestPathCount+",credit="+credit+")"

}