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
      jedis.set("CONTACTS_UNIQUE_" + key, "TRUE")
    })
  }
  
  def getAllData() : String = {
    val s = jedis.keys("CONTACTS_UNIQUE_*")
    "\n" + s.size +", " + s.toString() + "\n"
  }
}