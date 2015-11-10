package sql

import org.apache.spark.{SparkContext, SparkConf}

object SqlRunner {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Count Bytes Application")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc);

    import sqlContext.implicits._

    val airports = sc.textFile("d:\\logs\\airports.csv")
      .map(_.split(","))
      .map(a => {
      new Airport(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toInt, a(4).toInt,
        a(5).toInt, a(6).toInt, a(7).toInt, a(8), a(9).toInt, a(10), a(11).toInt, a(12).toInt,
        a(13).toInt, a(14).toInt, a(15).toInt, a(16), a(17), a(18).toInt, a(19).toInt, a(20).toInt,
        a(21).toInt, a(22).toInt, a(23).toInt, a(24).toInt, a(25).toInt, a(26).toInt, a(27).toInt, a(28).toInt)
    }).toDF()

    val flights = sc.textFile("d:\\logs\\planes.csv")
      .map(_.split(","))
      .map(a => {
      new Flight(a(0), a(1), a(2), a(3), a(4),
        a(5), a(6))
    }).toDF()

    val carriers = sc.textFile("d:\\logs\\carriers.csv").map(_.split(",")).map(a => {
      new Carriers(a(0), a(1))
    }).toDF()

    carriers.registerTempTable("carriers")
    flights.registerTempTable("flights")
    airports.registerTempTable("airports")


    val flightsCount = sqlContext.sql("select count(f.FlightNum), f.uniqueCarrier from flights f GROUP BY f.uniqueCarrier;")
    val nyCount = sqlContext.sql("select sum(flightNum) from (select count (f.flightNum) as flightNum  from flights f inner join airports a on f.origin  = a.iata where f.Month = 6 and a.city = 'New York' UNION ALL select count (f.flightNum)  as flightNum  from flights f inner join airports a on f.dest  = a.iata where f.Month = 6 and a.city = 'New York') as flights_count;")

  }
}
