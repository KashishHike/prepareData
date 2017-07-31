package com.hike

import java.sql.{Connection,DriverManager}

object MySql extends App {
  var connection:Connection = null
  
    
  def createConnection() {
    
    val host = "10.0.7.141"
    val port = 3306
    val username = "hike"
    val password = "h1kerS3my59l"
    
    // connect to the database named "mysql" on port 8889 of localhost
    val url = "jdbc:mysql://" + host + ":" + port + "/users?autoReconnect=true"
    println(url)
    val driver = "com.mysql.jdbc.Driver"
    try {
        Class.forName(driver)
        connection = DriverManager.getConnection(url, username, password)
    } catch {
        case e: Exception => {
          e.printStackTrace
          throw new RuntimeException("Could not create connection")
        }
    }
  }
  
  def getUserIdFromMsisdn(msisdn: String): String = {
    var uid = "null"
    val statement = connection.createStatement
    val queryMsisdn = msisdn.filterNot(_ == '+')
    val rs = statement.executeQuery("SELECT uid FROM devices where msisdn=" + queryMsisdn + " and end_time<=0")
    while (rs.next) {
        uid = rs.getString("uid")
    }
    uid
  }
  
  def closeConnection() = {
    connection.close
  }
}