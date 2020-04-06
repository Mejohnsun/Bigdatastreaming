package com.yidejia.streaming

import java.sql.ResultSet

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

/**
  * Oat_customer_seETL
  *  TODO
  *  2020/3/23 16:15
  *  by MS
  */
object OatCustomerSeETL {
    private val groupid = "OatCustomerSe"

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setAppName("Oat_customerETL")
//            .setMaster("local[1]")
            .set("spark.streaming.kafka.maxRatePerPartition", "300")
            .set("spark.streaming.backpressure.enabled", "true")
            .set("spark.streaming.stopGracefullyOnShutdown", "true")
        val ssc = new StreamingContext(sparkConf, Seconds(5))
        val topics = Array(Constant.TOPIC_CUSTOMER_SE)
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
        val customerSeDStream = SourceDStream.filter(item => {
            val obj = ParseJsonData.getJsonData(item.value())
            obj.isInstanceOf[JSONObject]
        }).mapPartitions(partitions=>{
            partitions.map {
                item =>
                    val json = item.value()
                    val JsonObject = ParseJsonData.getJsonData(json)
                    val customer_id = JsonObject.getJSONArray("data").getJSONObject(0).getLongValue("customer_id_se")
                    val trace_staff_id = JsonObject.getJSONArray("data").getJSONObject(0).getInteger("trace_staff_id")
                    val recom_staff_id = JsonObject.getJSONArray("data").getJSONObject(0).getInteger("recom_staff_id")
                    val serve_staff_id = JsonObject.getJSONArray("data").getJSONObject(0).getInteger("serve_staff_id")
                    (customer_id, trace_staff_id, recom_staff_id, serve_staff_id)
            }
        })
        customerSeDStream.print()
       customerSeDStream.print()
        customerSeDStream.foreachRDD(rdd => {
            rdd.foreachPartition(partition => {
                val sqlProxy = new SqlProxy()
                val connection = DataSourceUtil.getConnection
                val sql =
                    """
                      |select id
                      | from bigdata_profile.customer_info
                      |where customer_id=?
                    """.stripMargin
                val SqlCustomerSeUpdate =
                    """
                      |update bigdata_profile.customer_info
                      |set customer_id=?,trace_staff_id=?,recom_staff_id=?,serve_staff_id=?
                      |where customer_id = ?
                    """.stripMargin
                val SqlCustomerSeInsert =
                    """
                      |insert into
                      | bigdata_profile.customer_info(customer_id,trace_staff_id,recom_staff_id,serve_staff_id)
                      |values(?,?,?,?)
                    """.stripMargin
                try {
                    partition.foreach(item => {
                        if (connection != null) {
                            val statement = connection.prepareStatement(sql)
                            try {
                                connection.setAutoCommit(false)
                                statement.setLong(1, item._1)
                                val resultSet = statement.executeQuery()
                                if (resultSet.next()) {
                                    val scsu = connection.prepareStatement(SqlCustomerSeUpdate)
                                    scsu.setLong(1, item._1)
                                    scsu.setInt(2, item._2)
                                    scsu.setInt(3, item._3)
                                    scsu.setInt(4, item._4)
                                    scsu.setLong(5, item._1)
                                    scsu.executeUpdate()
                                    scsu.close()
                                } else {
                                    val scsi = connection.prepareStatement(SqlCustomerSeInsert)
                                    scsi.setLong(1, item._1)
                                    scsi.setInt(2, item._2)
                                    scsi.setInt(3, item._3)
                                    scsi.setInt(4, item._4)
                                    scsi.executeUpdate()
                                    scsi.close()
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
}
