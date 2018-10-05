package res.tasks

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.desc
import res.Main.ss
import res.entities
import res.entities.{ContinentCountryMarketCount, Train}

/**
  * Find top 3 hotels where people with children are interested but not booked in the end.
  */

object TrainTask3 extends IDiffAPI[ContinentCountryMarketCount] {
  override def byRDD(data: Dataset[Train]): Array[ContinentCountryMarketCount] = {
    data.rdd
      .filter(t => t.is_booking.contains(0))
      .filter(t => t.srch_adults_cnt.exists(_ >= 2))
      .groupBy(t => (t.hotel_continent, t.hotel_country, t.hotel_market))
      .map {
        case ((hotel_continent, hotel_country, hotel_market), it) =>
          entities.ContinentCountryMarketCount(hotel_continent, hotel_country, hotel_market, it.size)
      }
      .sortBy(_.count, ascending = false)
      .take(3)
  }

  override def byDF(data: Dataset[Train]): Array[ContinentCountryMarketCount] = {
    import ss.implicits._
    data.toDF()
      .filter($"is_booking" === 0 and $"srch_adults_cnt" >= 2)
      .groupBy("hotel_continent", "hotel_country", "hotel_market")
      .count()
      .sort(desc("count"))
      .limit(3)
      .as[ContinentCountryMarketCount].collect()
  }

  override def byDS(data: Dataset[Train]): Array[ContinentCountryMarketCount] = {
    import ss.implicits._
    data
      .filter(t => t.is_booking.contains(0))
      .filter(t => t.srch_adults_cnt.exists(_ >= 2))
      .groupByKey(t => (t.hotel_continent, t.hotel_country, t.hotel_market))
      .mapGroups {
        case ((hotel_continent, hotel_country, hotel_market), it) =>
          ContinentCountryMarketCount(hotel_continent, hotel_country, hotel_market, it.size)
      }
      .orderBy(desc("count"))
      .limit(3).collect()
  }
}
