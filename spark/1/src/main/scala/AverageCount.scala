import java.io._

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.matching.Regex

object AverageCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Count Bytes Application")
    val ctx = new SparkContext(conf)
    var accumulators = createAccumulators(ctx);

    val parsedLogs = parseLogs(ctx, accumulators)
    val countMap = parsedLogs.mapValues(word => (word, 1))
    val reducedData = countMap.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    val sum = reducedData.mapValues { case (sum, count) => (sum) / count}
    sum.saveAsTextFile("d:\\logs\\output.txt")
    saveAccumulators(accumulators);
  }

  def parseLogs(ctx: SparkContext, accumulators: scala.collection.mutable.Map[String, Int]): RDD[(String, Int)] = {
    val logData = ctx.textFile("d:\\logs\\000000")
    val ipPattern = new Regex("^ip[\\d]+")
    val bytesPattern = new Regex("\\d{3}+ \\d+")

    val parsedData = logData.map(line => {
      calculateBrowsers(accumulators, line)
      val ip = ipPattern.findFirstIn(line).getOrElse("none")
      var bytes = "";
      val ipBytes = bytesPattern.findAllMatchIn(line).foreach { digits => bytes =
        digits.group(0).split(" ") {
          1
        }
      }
      (ip, bytes.toInt)
    })
    return parsedData
  }

  def calculateBrowsers(browsers: scala.collection.mutable.Map[String, Int], line: String): Unit = {
    var key = "Other";
    if (line.contains("Mozilla")) {
      key = "Mozilla"
    } else if (line.contains("Opera")) {
      key = "Opera"
    } else if (line.contains("Chrome")) {
      key = "Chrome"
    }

    val value = browsers.get(key).getOrElse(0)
    var nextValue = value + 1
    browsers.put(key, nextValue)
  }

  def createAccumulators(ctx: SparkContext): scala.collection.mutable.Map[String, Int] = {
    val browsers = scala.collection.mutable.Map[String, Int]()
    browsers.put("Mozilla", 0)
    browsers.put("Opera", 0)
    browsers.put("Other", 0)
    return browsers;
  }

  def saveAccumulators(browsers: scala.collection.mutable.Map[String, Int]): Unit = {
    val fileWriter = new FileWriter("d:\\logs\\browsers.txt")
    /*  using(fileWriter) {
        browsers.foreach{
          x=> {fileWriter.write(data)}
        }
      }*/
  }

  def using[A <: {def close() : Unit}, B](param: A)(f: A => B): B =
    try {
      f(param)
    } finally {
      param.close()
    }
}
