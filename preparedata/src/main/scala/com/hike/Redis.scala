package com.hike

import redis.clients.jedis.Jedis
 
object Redis {
  
  val jedis = new Jedis("localhost");
  jedis.flushAll()
  
  def clear = jedis.flushAll()
  
  def putUniqueKeyToRedis(keys: List[String]) {
    if(keys.isEmpty)
      return
      
    println("Putting the data to redis")
    keys.foreach(key => {
      jedis.set(key, "TRUE")
    })
  }
  
  def getAllData() : String = {
    val s = jedis.keys("*")
    s.size() +", " + s.toString()
  }
}