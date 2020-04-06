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

object OatCustomerPffxETL {
    val SkinTypeList = List("", "中性", "干性", "混合性", "油性")
    val IsAllergyList = List("", "从不", "偶尔", "易过敏")
    val SkinColorList = List("", "白皙", "粉嫩", "暗黄", "大麦色")
    val ImproveTypeList = List("", "敏感/脆弱", "黑头粉刺/毛孔粗大/皮肤粗糙", "痘痘", "细纹/皮肤松弛",
        "暗黄/色斑", "黑眼圈/轻度眼袋/脂肪粒", "严重眼袋/眼纹")
    private val groupid = "OatCustomerPffx"

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setAppName("Oat_customerETL")
//            .setMaster("local[*]")
            .set("spark.streaming.kafka.maxRatePerPartition", "300")
            .set("spark.streaming.backpressure.enabled", "true")
            .set("spark.streaming.stopGracefullyOnShutdown", "true")
        val ssc = new StreamingContext(sparkConf, Seconds(5))
        val topics = Array(Constant.TOPIC_CUSTOMER_PFFX)
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
        val CustomerPffxDStream = SourceDStream.filter(item => {
            val obj = ParseJsonData.getJsonData(item.value())
            obj.isInstanceOf[JSONObject]
        }).map(
            item => {
                val json = item.value()
                val JsonObject = ParseJsonData.getJsonData(json)
                val customer_id = JsonObject.getJSONArray("data").getJSONObject(0).getLong("customer_id")
                val Skin_TypeNumber = JsonObject.getJSONArray("data").getJSONObject(0).getIntValue("skin_type")-1
                val skin_type = SkinTypeList(Skin_TypeNumber)
                val Is_AllergyNumber = JsonObject.getJSONArray("data").getJSONObject(0).getIntValue("is_allergy")-1
                val is_allergy = IsAllergyList(Is_AllergyNumber)
                val Skin_ColorNumber = JsonObject.getJSONArray("data").getJSONObject(0).getIntValue("skin_color")-1
                val skin_color = SkinColorList(Skin_ColorNumber)
                val Improve_TypeNumber = JsonObject.getJSONArray("data").getJSONObject(0).getIntValue("improve_type")-1
                val improve_type = ImproveTypeList(Improve_TypeNumber)
                (skin_type, is_allergy, skin_color, improve_type, customer_id)
            })
        CustomerPffxDStream.print()
        CustomerPffxDStream.foreachRDD(rdd => {
            rdd.foreachPartition(partition => {
                val sqlProxy = new SqlProxy()
                val connection = DataSourceUtil.getConnection
                val sql =
                    """
                      |select  customer_id
                      | from bigdata_profile.base_copy1
                      |where customer_id=?
                    """.stripMargin
                val SqlCustomerPffxUpdate =
                    """
                      |update bigdata_profile.oa_copy1
                      |set skin_type=?,is_allergy=?,skin_color=?,improve_type=?
                      |where customer_id = ?
                    """.stripMargin
                try {
                    partition.filter(item =>
                        item._5 != 0
                    ).foreach(item => {
                        if (connection != null) {
                            val statement = connection.prepareStatement(sql)
                            try {
                                connection.setAutoCommit(false)
                                statement.setLong(1, item._5)
                                val resultSet = statement.executeQuery()
                                if (resultSet.next()) {
                                    val scsu = connection.prepareStatement(SqlCustomerPffxUpdate)
                                    scsu.setString(1, item._1)
                                    scsu.setString(2, item._2)
                                    scsu.setString(3, item._3)
                                    scsu.setString(4, item._4)
                                    scsu.setLong(5, item._5)
                                    scsu.executeUpdate()
                                    scsu.close()
                                } else {
                                    println("customer_id:"+item._5+",在base表customer_id不存在，数据不处理")
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
