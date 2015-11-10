package sql

class Airport (year: Int, month: Int, dayOfMonth: Int, dayOfWeek: Int, depTime: Int, cRSDepTime: Int,
               arrTime: Int, cRSArrTime: Int, uniqueCarrier: String, flightNum: Int, tailNum: String,
               actualElapsedTime: Int, cRSElapsedTime: Int, airTime: Int, arrDelay: Int, depDelay: Int,
               origin: String, dest: String, distance: Int, taxiIn: Int, taxiOut: Int, cancelled: Int,
               cancellationCode: Int, diverted: Int, carrierDelay: Int, weatherDelay: Int, nASDelay: Int,
               securityDelay: Int, lateAircraftDelay: Int) extends Product {
  override def productElement(n: Int): Any = n match {
    case 0 => year
    case 1 => month
    case 2 => dayOfMonth
    case 3 => dayOfWeek
    case 4 => depTime
    case 5 => cRSDepTime
    case 6 => arrTime
    case 7 => cRSArrTime
    case 8 => uniqueCarrier
    case 9 => flightNum
    case 10 => tailNum
    case 11 => actualElapsedTime
    case 12 => cRSElapsedTime
    case 13 => airTime
    case 14 => arrDelay
    case 15 => depDelay
    case 16 => origin
    case 17 => dest
    case 18 => distance
    case 19 => taxiIn
    case 20 => taxiOut
    case 21 => cancelled
    case 22 => cancellationCode
    case 23 => diverted
    case 24 => carrierDelay
    case 25 => weatherDelay
    case 26 => nASDelay
    case 27 => securityDelay
    case 28 => lateAircraftDelay
    case _ => throw new IndexOutOfBoundsException(n.toString())
  }

  override def productArity: Int = 28

  override def canEqual(that: Any): Boolean = that.isInstanceOf[Airport]
}

