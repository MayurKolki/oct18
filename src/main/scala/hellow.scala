import org.apache.spark.SparkContext

object hellow {
  def main(args:Array[String]):Unit=
    {
     val sc =new SparkContext("local[4]","app_name_test")
     val input=sc.textFile("C:/Users/Admin/Desktop/aws Data engineer/test_data.txt")
      val rdd1=input.flatMap(x=>x.split(" "))
      val rdd2=rdd1.map(x=>(x,1))
      val rdd3=rdd2.reduceByKey((x,y)=>x+y)
      val rdd4=rdd3.sortBy(x=>x._2,false)
      rdd4.take(3).foreach(println)

scala.io.StdIn.readLine()

    }}
