package com.Emp.Detail.Inoutproj

import org.ekstep.analytics.framework.DataFetcher
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.util.CommonUtil

object MyDataFetcher {
  case class emp_del(emp_id: String, emp_name: String, Date: Long, time_in: Long, time_out: Long)
  def main(args: Array[String]): Unit = {

    implicit val sc = CommonUtil.getSparkContext(8, "Test Spark")
    val search = Fetcher("local", None, Option(Array(
      Query(None, None, None, None, None, None, None, None, None, Option("/home/tausif/office/emp.log")))));
    val rdd = DataFetcher.fetchBatchData[emp_del](search);
    rdd.foreach(println)
 
    println("\n\nEmp id & TOTAL NO OF Days\n")
    println("\n===============================")
    val x = rdd.map(x => (x.emp_id, 1)).reduceByKey((x, y) => x + y)
    x.foreach(println)

    val hs = rdd.map { x =>
            val hours= (CommonUtil.getTimeDiff(x.time_in, x.time_out).get)/3600
                        (x.emp_name,hours)
                     }
  println("\n\nEach emp & each day's hours")
  println("\n===============================")

    hs.foreach(println)

    
    println   ("\n\nEMP NAME  TOTAL HOURS\n")
    println("\n===============================")

    val y = hs.reduceByKey(_ + _)
          y.foreach(println)
          
            val max = y.map{x=>
                             (x._2)
                          }.max
              println("\n\n Maximum hours spend by emp:" +max)  
         val min = y.map{x=>
                             (x._2)
                          }.min
              println("\n\n Minimum hours spend by emp:" +min)                   
                          
   //val maxByHours = y.foldByKey(0.0)((acc,element)=> if(acc._2 > element._2) acc else element)
    
    
    val h =hs.groupBy(x=>x._1)
    //h.foreach(println)
    println("TOTAL RECORD===>" + rdd.count())
    println("***********************\n")

  }
}