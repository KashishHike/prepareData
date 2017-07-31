package com.hike

import redis.clients.jedis.Jedis
 
object Redis {
  
  val jedis = new Jedis("localhost");
  
  def putUniqueKeyToRedis(keys: List[String]) {
    if(keys.isEmpty)
      return
      
    println("Putting the data to redis")
    keys.foreach(key => {
      val uid = key.split(":")(0)
      val msisdn = key.split(":")(1)
      if(uid.equals("null"))
          jedis.set("UNIQUE_CONTACTS_" + msisdn, msisdn)
      else
        jedis.set("UNIQUE_CONTACTS_" + uid, msisdn)
    })
  }
  
  def getAllData() : String = {
    val allKeys = jedis.keys("UNIQUE_CONTACTS_*")
    allKeys.size +", " + allKeys.toString() + "\n"
  }
  
  def getValue(key:String): String = {
    jedis.get(key)
  }
}