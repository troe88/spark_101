package res.entities

case class ContinentCountryMarketCount(
                                        hotel_continent: Option[Long],
                                        hotel_country: Option[Long],
                                        hotel_market: Option[Long],
                                        count: Long
                                      ) {
  override def toString: String = {
    s"hotel_continent:${hotel_continent.get}, " +
      s"hotel_country ${hotel_country.get}, " +
      s"hotel_market:${hotel_market.get}, " +
      s"count:$count;"
  }
}