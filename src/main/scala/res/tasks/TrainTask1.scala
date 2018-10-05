package res.tasks

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import res.Main.ss
import res.entities
import res.entities.{ContinentCountryMarketCount, Train}

/**
  * Find top 3 most popular hotels between couples.
  * (treat hotel as composite key of continent, country and market).
  */

object TrainTask1 extends IDiffAPI[ContinentCountryMarketCount] {
  override def byRDD(data: Dataset[Train]): Array[ContinentCountryMarketCount] = {
    data.rdd
      .filter(t => t.srch_adults_cnt.contains(2))
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
      .filter($"srch_adults_cnt" === 2)
      .groupBy("hotel_continent", "hotel_country", "hotel_market")
      .count()
      .sort(desc("count"))
      .limit(3)
      .as[ContinentCountryMarketCount].collect()
  }

  override def byDS(data: Dataset[Train]): Array[ContinentCountryMarketCount] = {
    import ss.implicits._

    data
      .filter(t => t.srch_adults_cnt.contains(2))
      .groupByKey(t => (t.hotel_continent, t.hotel_country, t.hotel_market))
      .mapGroups {
        case ((hotel_continent, hotel_country, hotel_market), it) =>
          entities.ContinentCountryMarketCount(hotel_continent, hotel_country, hotel_market, it.size)
      }
      .orderBy(desc("count"))
      .limit(3).collect()
  }
}
