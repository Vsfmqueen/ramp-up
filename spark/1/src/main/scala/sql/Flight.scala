package sql

class Flight(iata: String, airport: String, city: String, state: String, country: String, lat: String, long: String) extends Product {
  override def productElement(n: Int): Any = n match {
    case 0 => iata
    case 1 => airport
    case 2 => city
    case 3 => state
    case 4 => country
    case 5 => lat
    case 6 => long
    case _ => throw new IndexOutOfBoundsException(n.toString())
  }

  override def productArity: Int = 24

  override def canEqual(that: Any): Boolean = that.isInstanceOf[Flight]
}
