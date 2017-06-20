package com.Emp.Detail.Inoutproj
    import org.apache.spark.{SparkConf, SparkContext}


object CarExample {
  def main(args: Array[String]): Unit = {

// 1. Create Spark configuration
val conf = new SparkConf().setAppName("SparkMe Application").setMaster("local[*]")
val sc = new SparkContext(conf)
//define RDD with raw data
val rawData=sc.textFile("/home/tausif/Desktop/DATA/cars.txt")

//map columns and datatypes
case class cars(make:String, model:String, mpg:Integer, cylinders :Integer, engine_disp:Integer, horsepower:Integer, weight:Integer ,accelerate:Double,	year:Integer, origin:String)

val carsData=rawData.map(x=>x.split("\t")).map(x=>cars(x(0).toString,x(1).toString,x(2).toInt,x(3).toInt,x(4).toInt,x(5).toInt,x(6).toInt,x(7).toDouble,x(8).toInt,x(9).toString))

carsData.take(2).foreach(println)

//persist to memory
carsData.cache()

//count cars origin wise
val x=carsData.map(x=>(x.origin,1)).reduceByKey((x,y)=>x+y)
x.foreach(println)

//filter out american cars
val americanCars=carsData.filter(x=>(x.origin=="American"))

println("Count total american cars==>")
println(americanCars.count())


println("Take sum of weights according to make==>")
val makeWeightSum=americanCars.map(x=>(x.make,x.weight.toInt)).combineByKey((x:Int) => (x, 1),(acc:(Int, Int), x) => (acc._1 + x, acc._2 + 1),                                                                                                                
(acc1:(Int, Int), acc2:(Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
makeWeightSum.foreach(println)


//take average
val makeWeightAvg=makeWeightSum.map(x=>(x._1,(x._2._1/x._2._2)))
makeWeightAvg.collect()
    
  }
}