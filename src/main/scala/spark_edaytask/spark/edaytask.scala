package spark_edaytask.spark
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object edaytask {
  
  def main(args:Array[String]):Unit={  

    val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
       val sc = new SparkContext(conf) 
        sc   .setLogLevel("Error")
        
         val data = sc.textFile("/Users/nandinipatnaik/Documents/putfile/txns")
       val gym_data = data.filter(x=>x.contains("Chicago"))
       val cash_data = gym_data.filter(x=>x.contains("credit"))
       val flat_data = cash_data.flatMap(x=>x.split(","))
       println(flat_data)
       //flat_data.saveAsTextFile("hdfs:/user/cloudera/txn_Chicago")
       
  }
}