package uwt.socialnetworks
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import scala.reflect.ClassTag
import org.apache.velocity.runtime.directive.Foreach
import akka.dispatch.Foreach
import java.io._

object TestIterativeMap extends App {
	val sc: SparkContext = new SparkContext("spark://master:7077", "SparkPi", "/home/spark/spark0.9", Array("/home/spark/apps/betweenness/betweenness.jar"))
	var arr = Array(1,2)
	var rdd = sc.parallelize(arr, 7)
	
	
	for(i<-1 to 10)
	{
		rdd = rdd.map(t=>{println(t)
			val writer = new PrintWriter(new File("log.txt"))
			for(j<- 1 to 100)
			{
				
				writer.write(i +":"+t+ "\n")
			}
			writer.close()
			t
		})
	}
	
	rdd.collect
	

}