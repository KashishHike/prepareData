package com.hike

import redis.clients.jedis.Jedis
 
object Redis {
  
  val jedis = new Jedis("localhost");
  
  def clear = jedis.flushAll()
  
  def putUniqueKeyToRedis(keys: List[String]) {
    if(keys.isEmpty)
      return
      
    println("Putting the data to redis")
    keys.foreach(key => {
      jedis.set("UNIQUE_CONTACTS_" + key, "TRUE")
    })
  }
  
  def getAllData() : String = {
    val allKeys = jedis.keys("UNIQUE_CONTACTS_*")
    allKeys.size +", " + allKeys.toString() + "\n"
  }
}