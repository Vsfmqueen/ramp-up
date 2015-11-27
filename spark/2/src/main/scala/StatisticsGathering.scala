import java.util.Calendar

import encoder.PackageInfoDecoder
import kafka.serializer.StringDecoder
import model.{PackageInfo, Statistics}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


object StatisticsGathering {

  def main(args: Array[String]): Unit ={
    val conf = new SparkConf()
      .setAppName("Statistics reader")
      .setMaster("local[4]")
      .set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf)

    val kafkaConsumerSSC = new StreamingContext(conf, Seconds(10))

    val hbaseHelper = new HbaseHelper()

    val kafkaConf = Map(
      "metadata.broker.list" -> "sandbox.hortonworks.com:9092",
      "zookeeper.connect" -> "sandbox.hortonworks.com:2181",
      "group.id" -> "123",
      "zookeeper.connection.timeout.ms" -> "1000")

    val kafkaTopics =  Map("ips" -> 1)
    val kafkaStream = KafkaUtils.createStream[String, PackageInfo, StringDecoder, PackageInfoDecoder](kafkaConsumerSSC, kafkaConf, kafkaTopics, StorageLevel.MEMORY_ONLY_SER)

    kafkaStream.print(100)

    val today = Calendar.getInstance().getTime()

    val statistics = kafkaStream.map(x=>{
      (x._2.ip, x._2)
    }).mapValues(x=>{(x, 1)})
      .reduceByKey((x, y)=>{
        val bytes = x._1.totalBytesSize+y._1.totalBytesSize
        ( new PackageInfo(x._1.ip, bytes), x._2+ y._2)})
      .map(x=>{
          val totalBytes = x._2._1.totalBytesSize
          var averageSpeed = totalBytes/3600*1000000
          new Statistics(x._1, today, totalBytes, averageSpeed)
      })

    hbaseHelper.putStatistics(statistics)
    kafkaConsumerSSC.start()
    kafkaConsumerSSC.awaitTermination()
  }

}
