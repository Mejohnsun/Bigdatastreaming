package com.yidejia.util

import java.io.InputStream
import java.util.Properties

import scala.collection.mutable


object PropertiesUtil {
    
    val map: mutable.Map[String, Properties] = mutable.Map[String, Properties]()
    
    def getProperty(configFile: String, propName: String): String = {
        
        map.getOrElseUpdate(configFile, {
            println("YidejiaBigData")
            
            val is: InputStream = getClass.getClassLoader.getResourceAsStream(configFile)
            val ps: Properties = new Properties()
            ps.load(is)
            ps
        }).getProperty(propName)
    }
    
    def main(args: Array[String]): Unit = {
        println(getProperty("config.properties", "kafka.broker.list"))
    }
}
