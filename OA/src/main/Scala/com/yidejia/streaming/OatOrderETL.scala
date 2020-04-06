package com.yidejia.streaming

import java.sql.ResultSet
import java.text.SimpleDateFormat
import java.util.Date
import com.alibaba.fastjson.JSONObject
import com.yidejia.bean.Constant
import com.yidejia.util._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import scala.collection.mutable

object OatOrderETL {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    private val groupid = "OatOrder"

    def main(args: Array[String]): Unit = {
        val SparkConf = new SparkConf().setAppName("OatOrderETL")
//            .setMaster("local[*]")
            .set("spark.streaming.kafka.maxRatePerPartition", "300")
            .set("spark.streaming.backpressure.enabled", "true")
            .set("spark.streaming.stopGracefullyOnShutdown", "true")
        val ssc = new StreamingContext(SparkConf, Seconds(5))
        val topics = Array(Constant.TOPIC_ORDER)
        val kafkaMap: Map[String, Object] = Map[String, Object](
            "bootstrap.servers" -> "172.16.50.247:9092,172.16.50.246:9092,172.16.50.246:9092",
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> groupid,
            "auto.offset.reset" -> "earliest",
            "enable.auto.commit" -> (false: java.lang.Boolean)
        )
        //查询mysql是否存在偏移量
        val sqlProxy = new SqlProxy()
        val offsetMap = new mutable.HashMap[TopicPartition, Long]()
        val client = DataSourceUtil.getConnection
        try {
            sqlProxy.executeQuery(client, "select *from `offset_manager` where groupid=?", Array(groupid), new QueryCallback {
                override def process(rs: ResultSet): Unit = {
                    while (rs.next()) {
                        val model = new TopicPartition(rs.getString(2), rs.getInt(3))
                        val offset = rs.getLong(4)
                        offsetMap.put(model, offset)
                    }
                    rs.close()
                }
            })
        } catch {
            case e: Exception => e.printStackTrace()
        } finally {
            sqlProxy.shutdown(client)
        }
        val SourceDStream: InputDStream[ConsumerRecord[String, String]] = if (offsetMap.isEmpty) {
            KafkaUtils.createDirectStream(
                ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaMap))
        } else {
            KafkaUtils.createDirectStream(
                ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaMap, offsetMap))
        }
        val OrderDStream = SourceDStream.filter(item => {
            val obj = ParseJsonData.getJsonData(item.value())
            obj.isInstanceOf[JSONObject]
        }).mapPartitions(partitions => {
            partitions.map {
                item =>
                    val json = item.value()
                    val JsonObject = ParseJsonData.getJsonData(json)
                    val customer_id = JsonObject.getJSONArray("data").getJSONObject(0).getLong("customer_id")
                    val order_id = JsonObject.getJSONArray("data").getJSONObject(0).getLong("order_id")
                    val submit_time = JsonObject.getJSONArray("data").getJSONObject(0).getLong("submit_time").toInt
                    val ship_date = JsonObject.getJSONArray("data").getJSONObject(0).getDate("ship_date")
                    val ship_time = JudgeNull(ship_date)
                    val sign_date = JsonObject.getJSONArray("data").getJSONObject(0).getDate("sign_date")
                    val sign_time = JudgeNull(sign_date)
                    val status = JudgeStatus(sign_time, ship_time, submit_time)
                    (customer_id, order_id, submit_time, ship_time, sign_time, status)
            }
        })
        OrderDStream.print()
        OrderDStream.foreachRDD(rdd => {
            rdd.foreachPartition(partition => {
                val sqlProxy = new SqlProxy()
                val connection = DataSourceUtil.getConnection
                val sql =
                    """
                      |select  order_id
                      | from bigdata_profile.order_status
                      |where order_id=?
                    """.stripMargin
                val SqlMaxOrderId =
                    """
                      |select  max(order_id)
                      |from bigdata_profile.order_status
                    """.stripMargin
                val SqlOrderStatusUpdate =
                    """
                      |update bigdata_profile.order_status
                      |set order_id=?,submit_time=?,ship_time=?,sign_time=?,status=?
                      |where customer_id = ?
                    """.stripMargin
                val SqlOrderStatusInsert =
                    """
                      |insert into
                      | bigdata_profile.order_status(customer_id,order_id,submit_time,ship_time,sign_time,status)
                      |values(?,?,?,?,?,?)
                    """.stripMargin
                try {
                    partition.foreach(item => {
                        if (connection != null) {
                            val statement = connection.prepareStatement(sql)
                            try {
                                connection.setAutoCommit(false)
                                statement.setLong(1, item._2)
                                val resultSet = statement.executeQuery()
                                if (resultSet.next()) {
                                    val sosu = connection.prepareStatement(SqlOrderStatusUpdate)
                                    sosu.setLong(1, item._2)
                                    sosu.setInt(2, item._3)
                                    sosu.setInt(3, item._4)
                                    sosu.setInt(4, item._5)
                                    sosu.setString(5, item._6)
                                    sosu.setLong(6, item._1)
                                    sosu.executeUpdate()
                                    sosu.close()
                                } else {
                                    val MaxOrderId = connection.prepareStatement(SqlMaxOrderId)
                                    val result = MaxOrderId.executeQuery()
                                    while (result.next()) {
                                        val Order_id = result.getLong(1)
                                        if (Order_id < item._2) {
                                            val sosi = connection.prepareStatement(SqlOrderStatusInsert)
                                            sosi.setLong(1, item._1)
                                            sosi.setLong(2, item._2)
                                            sosi.setInt(3, item._3)
                                            sosi.setInt(4, item._4)
                                            sosi.setInt(5, item._5)
                                            sosi.setString(6, item._6)
                                            sosi.executeUpdate()
                                            sosi.close()
                                        } else {
                                            println("order_id:"+item._2+"小于最大的order_id，数据不做新增处理")
                                        }
                                    }
                                    result.close()
                                }
                                resultSet.close()
                                connection.commit()
                            } catch {
                                case e: Exception => e.printStackTrace()
                            } finally {
                                statement.close()
                            }
                        }
                    })
                } catch {
                    case e: Exception => e.printStackTrace()
                } finally {
                    sqlProxy.shutdown(connection)
                }
            })
        })
        SourceDStream.foreachRDD(rdd => {
            val sqlProxy = new SqlProxy()
            val client = DataSourceUtil.getConnection
            try {
                val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                for (or <- offsetRanges) {
                    sqlProxy.executeUpdate(client, "replace into `offset_manager` (groupid,topic,`partition`,untilOffset) values(?,?,?,?)",
                        Array(groupid, or.topic, or.partition.toString, or.untilOffset))
                }
            } catch {
                case e: Exception => e.printStackTrace()
            } finally {
                sqlProxy.shutdown(client)
            }
        })
        ssc.start()
        ssc.awaitTermination()
    }

    def JudgeStatus(sign_time: Integer, ship_time: Integer, submit_time: Long): String = {
        if (sign_time == 0) {
            " "
        } else if (ship_time == 0) {
            "已提交"
        } else if (submit_time == 0) {

            "已发货"
        } else {
            "已签收"
        }

    }

    def JudgeNull(time: Date): Int = {
        if (time != null) {
            (sdf.parse(sdf.format(time)).getTime / 1000).toInt
        } else {
            0
        }

    }

}
