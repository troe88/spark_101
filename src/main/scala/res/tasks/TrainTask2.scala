package res.tasks

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.desc
import res.Main.ss
import res.entities
import res.entities.{CountryCount, Train}

/**
  * Find the most popular country where hotels are booked and searched from the same country.
  */

object TrainTask2 extends IDiffAPI[CountryCount] {
  override def byRDD(data: Dataset[Train]): Array[CountryCount] = {
    data.rdd
      .filter(t => t.srch_rm_cnt.contains(t.cnt.get))
      .groupBy(t => t.cnt)
      .map {
        case (cnt, it) =>
          entities.CountryCount(cnt, it.size)
      }
      .sortBy(_.count, ascending = false)
      .take(3)
  }

  override def byDF(data: Dataset[Train]): Array[CountryCount] = {
    import ss.implicits._

    data.toDF()
      .filter($"srch_rm_cnt" === $"cnt")
      .groupBy("cnt")
      .count()
      .sort(desc("count"))
      .limit(3)
      .as[CountryCount].collect()
  }

  override def byDS(data: Dataset[Train]): Array[CountryCount] = {
    import ss.implicits._

    data
      .filter(t => t.srch_rm_cnt.contains(t.cnt.get))
      .groupByKey(_.cnt)
      .mapGroups({
        case (cnt, it) =>
          entities.CountryCount(cnt, it.size)
      })
      .sort(desc("count"))
      .limit(3).collect()
  }
}
