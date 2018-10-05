package res.tasks

import org.apache.spark.sql.Dataset
import res.entities.Train

trait IDiffAPI[T] {
  def byRDD(data: Dataset[Train]): Array[T]

  def byDF(data: Dataset[Train]): Array[T]

  def byDS(data: Dataset[Train]): Array[T]
}
