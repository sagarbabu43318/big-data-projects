package src.bin

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

object sc {

    case class Nodeweights(src: Int, tgt: Int, weight:Int)

	def main(args: Array[String]) {

    	val sc = new SparkContext(new SparkConf().setAppName("sc"))

		val sqlContext = new SQLContext(sc)

		import sqlContext.implicits._

    	val file = sc.textFile("hdfs://localhost:8020" + args(0)) // read the file

        val var0 = file.map(_.split("\t")).map(p => Nodeweights(p(0).trim.toInt,
            p(1).trim.toInt,p(2).trim.toInt)).toDF()

        var0.registerTempTable("var0")

        val var1 = var0.filter($"weight" > 4) // filter out weights less than 5

        val var2 = var1.groupBy("tgt").agg(sum("weight") as "w1") //grp by tgt

        val var3 = var2.withColumnRenamed("tgt","node")

        val var4 = var1.groupBy("src").agg(sum("weight") as "w2") // grp by src

        val var5 = var4.withColumnRenamed("src","node")

        val var6 = var3.join(var5, Seq("node"), "outer") // perform an inner join on var3 and var5

        val var7 = var6.na.fill(0, Seq("w1")) // replace null values with 0

        val var8 = var7.na.fill(0, Seq("w2")) // replace null values with 0

        val var9 = var8.select($"node", $"w1" - $"w2" as "diff") // calculate net node weight

    	var9.map(x => x.mkString("\t")).saveAsTextFile("hdfs://localhost:8020" + args(1))
  	}
}
