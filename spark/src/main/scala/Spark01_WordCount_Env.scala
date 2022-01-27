import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount_Env {
	Logger.getLogger("org").setLevel(Level.ERROR)
	def main(args: Array[String]): Unit = {
		val conf: SparkConf = new SparkConf().setMaster("local").setAppName("wordcountEnv")
		val sc = new SparkContext(conf)
		val word: RDD[String] = sc.textFile("data/word.txt")
		val wordRdd: RDD[String] = word.flatMap(_.split(" "))
		val result = wordRdd.groupBy(x=>x).mapValues(_.size)
		result.collect().foreach(println)
		sc.stop()
	}
}
