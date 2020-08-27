import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.mutable.ListBuffer
import scala.math.min
import org.apache.spark.rdd.RDD

object Graph {
    def main(args: Array[ String ]) {
        val conf = new SparkConf().setAppName("Graph")
        val sc = new SparkContext(conf)
        val graph = sc.textFile(args(0)).map(line=>{val a=line.split(',')
                                        (a(0).toLong, a(0).toLong, a.slice(1, a.length).toList.map(_.toLong))})
        val i=0
        def mapper(item:(Long,Long,List[Long])) = {
            var list = new ListBuffer[(Long,Long)]
        list+=(((item._1).toLong,(item._2).toLong))
        if(item._2 > 0) {
            for(x<-item._3)
                list+=(((x).toLong,(item._2).toLong))
        }
        list
    }
    def reducer(item:(Long,(Long,(Long,List[Long]))))={
        var groupID =  item._2._2._1
        if(item._2._2._1 == (0).toLong){
            groupID = item._2._1
        }
        (item._1,groupID,item._2._2._2)
    }
        val gh=graph
        for(a <- 1 until 5) {
        val id=graph.map(v =>((v._1,v._2) ,v._3.map(x=>(x,v._2))))
        val VID=id.map(x=>x._1)
        val list=id.map(x=>x._2).flatMap(x=>x)
        val adj=list.union(VID) 
        val group=adj.reduceByKey((x,y)=> Math.min(x,y)).join(graph.map(x=>(x._1,(x._2,x._3)))).map(x=>(x._1,x._2._1,x._2._2._2)) 
        //graph = group.map(x=>(x._1,x._2._1,x._2._2._2)) 
        val gh = group.flatMap{ mapper(_)}
            .reduceByKey(_ min _)
            .join( graph.map(k => {(k._1,(k._2,k._3))}) )
            .map{ reducer(_) }
        }
        val grp = gh.map(graph=>(graph._2,1)).reduceByKey(_+_)
        grp.collect().foreach(println)
        sc.stop()
    }
}