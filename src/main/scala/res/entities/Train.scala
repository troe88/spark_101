package res.entities

import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp

case class Train(
                  // TODO Timestamp not working with 'yyyy-mm-dd hh:mm:ss'
                  // date_time: Option[Timestamp],
                  date_time: Option[String],
                  site_name: Option[Long],
                  posa_continent: Option[Long],
                  user_location_country: Option[Long],
                  user_location_region: Option[Long],
                  user_location_city: Option[Long],
                  orig_destination_distance: Option[Float],
                  user_id: Option[Long],
                  is_mobile: Option[Long],
                  is_package: Option[Long],
                  channel: Option[Long],
                  srch_ci: Option[String],
                  srch_co: Option[String],
                  srch_adults_cnt: Option[Long],
                  srch_children_cnt: Option[Long],
                  srch_rm_cnt: Option[Long],
                  srch_destination_id: Option[Long],
                  srch_destination_type_id: Option[Long],
                  is_booking: Option[Long],
                  cnt: Option[Long],
                  hotel_continent: Option[Long],
                  hotel_country: Option[Long],
                  hotel_market: Option[Long],
                  hotel_cluster: Option[Long]
                )