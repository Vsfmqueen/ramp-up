import org.apache.spark.{SparkConf, SparkContext}

object WorldCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Count Bytes Application")
    val sc = new SparkContext(conf)
    val logData = sc.textFile("d:\\logs\\000000")
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}
