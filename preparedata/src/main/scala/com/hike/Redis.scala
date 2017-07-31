package com.hike

import redis.clients.jedis.Jedis
 
object Redis {
  
  val jedis = new Jedis("localhost");
  
  def putUniqueKeyToRedis(keys: List[String]) {
    if(keys.isEmpty)
      return
      
    println("Putting the data to redis")
    keys.foreach(key => {
      jedis.set("uc:" + key, "")
    })
  }
  
  def getAllData() : String = {
    val allKeys = jedis.keys("uc:*")
    allKeys.size +", " + allKeys.toString() + "\n"
  }
  
  def getValue(key:String): String = {
    jedis.get(key)
  }
}
