package com.yidejia.streaming
import java.sql.{ResultSet, Statement}
import java.text.SimpleDateFormat
import com.alibaba.fastjson.JSONObject
import com.yidejia.bean.Constant
import com.yidejia.util._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf
import scala.collection.mutable

object OatCustomerETL {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    private val groupid = "OatCustomer"

    def main(args: Array[String]): Unit = {
        val SparkConf = new SparkConf().setAppName("Oat_customerETL")
//            .setMaster("local[*]")
            .set("spark.streaming.kafka.maxRatePerPartition", "300")
            .set("spark.streaming.backpressure.enabled", "true")
            .set("spark.streaming.stopGracefullyOnShutdown", "true")
        val ssc = new StreamingContext(SparkConf, Seconds(5))
        val topics = Array(Constant.TOPIC_CUSTOMER)
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

        val CustomerDStream = SourceDStream.filter(item => {
            val obj = ParseJsonData.getJsonData(item.value())
            obj.isInstanceOf[JSONObject]
        }).mapPartitions(partitions => {
            partitions.map(item => {
                val json = item.value()
                val jsonObject = ParseJsonData.getJsonData(json)
                //base_copy1
                val customer_id = jsonObject.getJSONArray("data").getJSONObject(0).getLong("customer_id")
                val name = jsonObject.getJSONArray("data").getJSONObject(0).getString("customer_name")
                val GenderNumber = jsonObject.getJSONArray("data").getJSONObject(0).getIntValue("gender")
                val gender = JudgeGender(GenderNumber)
                val calendar_number = jsonObject.getJSONArray("data").getJSONObject(0).getIntValue("calendar")
                val birthday = jsonObject.getJSONArray("data").getJSONObject(0).getString("birthday")
                val birth = JudgeCalender(calendar_number, birthday)
                val province = jsonObject.getJSONArray("data").getJSONObject(0).getIntValue("province")
                val city = jsonObject.getJSONArray("data").getJSONObject(0).getIntValue("city")
                val district = jsonObject.getJSONArray("data").getJSONObject(0).getString("district")
                val handset = jsonObject.getJSONArray("data").getJSONObject(0).getLongValue("handset")
                val handset2 = jsonObject.getJSONArray("data").getJSONObject(0).getLongValue("handset2")
                val qq_code = jsonObject.getJSONArray("data").getJSONObject(0).getLongValue("qq_code")
                val ty_code = jsonObject.getJSONArray("data").getJSONObject(0).getString("ty_code")
                //oa_copy1
                val total_cash = jsonObject.getJSONArray("data").getJSONObject(0).getBigDecimal("total_cash")
                val customer_grade = jsonObject.getJSONArray("data").getJSONObject(0).getIntValue("customer_grade")
                val customer_type = jsonObject.getJSONArray("data").getJSONObject(0).getIntValue("customer_type")
                val customer_source = jsonObject.getJSONArray("data").getJSONObject(0).getIntValue("customer_source")
                val total_score = jsonObject.getJSONArray("data").getJSONObject(0).getBigDecimal("total_score")
                val lock_score = jsonObject.getJSONArray("data").getJSONObject(0).getBigDecimal("lock_score")
                val used_score = jsonObject.getJSONArray("data").getJSONObject(0).getBigDecimal("used_score")
                val can_use_score = jsonObject.getJSONArray("data").getJSONObject(0).getBigDecimal("can_use_score")
                val last_trace_date = jsonObject.getJSONArray("data").getJSONObject(0).getString("last_trace_date")
                val traced_at=JudgeNull(last_trace_date)
                val locked_at_old = jsonObject.getJSONArray("data").getJSONObject(0).getString("lock_date")
                val locked_at=JudgeNull(locked_at_old)
                val fsale_date = jsonObject.getJSONArray("data").getJSONObject(0).getString("fsale_date")
                val first_sale_date=JudgeNull(fsale_date)
                ((customer_id, name, gender, birth._1, birth._2, province, city, district, handset, handset2, qq_code, ty_code),
                    (total_cash, customer_grade, customer_type, customer_source, total_score, lock_score, used_score, can_use_score, traced_at, locked_at, first_sale_date))
            })

        })
        CustomerDStream.print()
        CustomerDStream.foreachRDD(rdd => {
            rdd.foreachPartition(partition => {
                val sqlProxy = new SqlProxy()
                val connection = DataSourceUtil.getConnection
                val sql =
                    """
                      |select  id
                      | from bigdata_profile.base_copy1
                      |where customer_id=?
                    """.stripMargin
                val SqlBaseUpdate =
                    """
                      |update bigdata_profile.base_copy1
                      |set customer_id = ?,name =?,gender =?,birthday =?,luna_birthday =?,province =?,city =?,district=?,handset =?,handset2 =?,qq_code =?,ty_code =?
                      |where customer_id = ?
                    """.stripMargin
                val SqlOaUpdate =
                    """
                      |update bigdata_profile.oa_copy1
                      |set total_cash=?,customer_grade=?,customer_type=?,customer_source=?,total_score=?,lock_score=?,used_score=?,can_use_score=?,traced_at=?,locked_at=?,first_sale_date=?
                      |where base_id = ?
                    """.stripMargin

                val SqlBaseInsert =
                    """
                      |insert into
                      |bigdata_profile.base_copy1(customer_id,name,gender,birthday,luna_birthday,province,city,district,handset,handset2,qq_code,ty_code)
                      |values(?,?,?,?,?,?,?,?,?,?,?,?)
                    """.stripMargin
                val SqlOaInsert =
                    """
                      |insert into
                      | bigdata_profile.oa_copy1(total_cash,customer_grade,customer_type,customer_source,total_score,lock_score,used_score,can_use_score,traced_at,locked_at,first_sale_date,base_id,customer_id)
                      |values(?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """.stripMargin
                try {
                    partition.foreach(record => {
                        if (connection != null) {
                            val preparedStatement = connection.prepareStatement(sql)
                            try {
                                connection.setAutoCommit(false)
                                preparedStatement.setLong(1, record._1._1)
                                val resultSet = preparedStatement.executeQuery()
                                if (resultSet.next()) {
                                    val id = resultSet.getLong(1)
                                    val pst = connection.prepareStatement(SqlBaseUpdate)
                                    val sou = connection.prepareStatement(SqlOaUpdate)
                                    pst.setLong(1, record._1._1)
                                    pst.setString(2, record._1._2)
                                    pst.setInt(3, record._1._3)
                                    pst.setString(4, record._1._4)
                                    pst.setString(5, record._1._5)
                                    pst.setInt(6, record._1._6)
                                    pst.setInt(7, record._1._7)
                                    pst.setString(8, record._1._8)
                                    pst.setLong(9, record._1._9)
                                    pst.setLong(10, record._1._10)
                                    pst.setLong(11, record._1._11)
                                    pst.setString(12, record._1._12)
                                    pst.setLong(13,record._1._1)
                                    pst.executeUpdate()
                                    sou.setBigDecimal(1, record._2._1)
                                    sou.setInt(2, record._2._2)
                                    sou.setInt(3, record._2._3)
                                    sou.setInt(4, record._2._4)
                                    sou.setBigDecimal(5, record._2._5)
                                    sou.setBigDecimal(6, record._2._6)
                                    sou.setBigDecimal(7, record._2._7)
                                    sou.setBigDecimal(8, record._2._8)
                                    sou.setString(9, record._2._9)
                                    sou.setString(10, record._2._10)
                                    sou.setString(11, record._2._11)
                                    sou.setLong(12, id)
                                    sou.executeUpdate()
                                    pst.close()
                                    sou.close()
                                } else {
                                    val sbi = connection.prepareStatement(SqlBaseInsert,Statement.RETURN_GENERATED_KEYS)
                                    val soi = connection.prepareStatement(SqlOaInsert)
                                    sbi.setLong(1, record._1._1)
                                    sbi.setString(2, record._1._2)
                                    sbi.setInt(3, record._1._3)
                                    sbi.setString(4, record._1._4)
                                    sbi.setString(5, record._1._5)
                                    sbi.setInt(6, record._1._6)
                                    sbi.setInt(7, record._1._7)
                                    sbi.setString(8, record._1._8)
                                    sbi.setLong(9, record._1._9)
                                    sbi.setLong(10, record._1._10)
                                    sbi.setLong(11, record._1._11)
                                    sbi.setString(12,record._1._12)
                                    sbi.executeUpdate()
                                    val keys = sbi.getGeneratedKeys
                                    if(keys.next()){
                                        val base_id = keys.getLong(1)
                                        soi.setBigDecimal(1, record._2._1)
                                        soi.setInt(2, record._2._2)
                                        soi.setInt(3, record._2._3)
                                        soi.setInt(4, record._2._4)
                                        soi.setBigDecimal(5, record._2._5)
                                        soi.setBigDecimal(6, record._2._6)
                                        soi.setBigDecimal(7, record._2._7)
                                        soi.setBigDecimal(8, record._2._8)
                                        soi.setString(9, record._2._9)
                                        soi.setString(10, record._2._10)
                                        soi.setString(11, record._2._11)
                                        soi.setLong(12, base_id)
                                        soi.setLong(13,record._1._1)
                                        soi.executeUpdate()
                                        soi.close()
                                    }
                                    sbi.close()
                                    keys.close()
                                }
                                resultSet.close()
                                connection.commit()
                            } catch {
                                case e: Exception => e.printStackTrace()
                            } finally {
                                preparedStatement.close()
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

    def JudgeCalender(calendar: Int, birthday: String): (String, String) = {
      if(birthday==null){
          (""," ")
      }else{
          if (calendar == 0 || calendar == 2 || calendar == 3) {
              (birthday, " ")
          } else if (calendar == 1 || calendar == 4) {
              (" ", birthday)
          } else {
              (" ","")
          }

      }

    }

    def JudgeHandset2(number: Long): String = {


        if (number == 0) {
            "n"
        } else if (number == 1) {
            "y"
        } else {
            null
        }

    }

    def JudgeGender(number: Long): Int = {

        if (number == 2) {
            2
        } else if (number == 3) {
            1
        } else {
            0
        }

    }
    def JudgeNull(str:String): String ={
        if (str==null){
            " "
        }else{
            str
        }
    }
}
