package res

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.{Encoders, SparkSession}
import res.entities.Train
import res.tasks.{TrainTask1, TrainTask2, TrainTask3}

object Main {
  val conf: Config = ConfigFactory.load

  System.setProperty("hadoop.home.dir", conf.getString("winutils"))

  val ss: SparkSession = SparkSession.builder
    .master("local[*]")
    .appName("big data #101")
    .getOrCreate()
  ss.sparkContext.setLogLevel("WARN")
  val log: Logger = LogManager.getRootLogger

  def main(args: Array[String]): Unit = {
    try {
      import ss.implicits._
      val data = ss.read
        .option("header", "true")
//        .option("timestampFormat", "yyyy-mm-dd hh:mm:ss")
//        .option("dateFormat", "yyyy-mm-dd hh:mm:ss")
        .schema(Encoders.product[Train].schema)
        .csv(conf.getString("path_to_train"))
        .as[Train]

      List(
        TrainTask1,
        TrainTask2,
        TrainTask3
      )
        .map(t => Map(
        "df : " -> t.byDF(data),
        "ds : " -> t.byDS(data),
        "rdd: " -> t.byRDD(data)
      ))
        .map(_.map(m => s"${m._1} = ${m._2.deep.toString()}"))
        .foreach(_.foreach(log.warn))
    } finally {
      ss.close()
    }
  }

}
