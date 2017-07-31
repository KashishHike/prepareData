package com.hike

import java.io.FileWriter
import java.util.HashMap
import java.util.concurrent.Callable
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.reflect._
import scala.reflect.ClassTag
import java.lang.reflect.Type
import com.google.gson.reflect.TypeToken
import scala.collection.mutable.Map


import com.google.gson.Gson


object Main {
  
  var es: ExecutorService = null
  val prefix = "/home/kashish"
  val relationshipDumpFile = s"$prefix/relationships.csv"
  val errorFile = s"$prefix/errors"
  val numThreadsfileName = s"$prefix/numThreads"
  val inputFilename = s"$prefix/000000_0"
  val redisDumpFileLocation = s"$prefix/redisDump"
  val csvHeader = "sourceUid,sourceMsisdn,destUid,destMsisdn,isContact,isFriend,isOnHike\n" 
  
  def launchANewThread(uid: String): Future[List[String]] = {
    es.submit(new Callable[List[String]]() {
        def call(): List[String] = {
          println(s"Checking for: $uid")
          return getRecords(uid)
        }
    });
  }
  
  def getNewThreadCount : Int = {
    val newNumberOfThreads = Source.fromFile(numThreadsfileName).getLines.toList(0).trim
    if (newNumberOfThreads == null || newNumberOfThreads == "") {
      println(s"Invalid number of threads $newNumberOfThreads")
    }
    newNumberOfThreads.toInt
  }
  
  def getRecords(myUid :String): List[String] = {
    
    // Call the contacts api to get this user's contacts. Returns AB
    val myHikeContacts: ABResponse = callAbApi(myUid)
    
    // Get my msisdn
    val myMsisdn = myHikeContacts.msisdn
    
    if(myHikeContacts.stat.equalsIgnoreCase("fail"))
    	return List[String]()
      
    // Call the friends api to get this user's friends on hike. Returns List(Friendship)
    val myHikeFriends: Map[String, String] = callFriendsApi(myUid, myHikeContacts.msisdn)
    
    // Construct the record. Here the isContact is true
    val contactNodes = myHikeContacts.ab.asScala.toList.filterNot(ab => myUid.equals(ab.uid)).map(ab => {
      myUid + "," + myMsisdn + "," + ab.uid + "," + ab.msisdn + ",true," + myHikeFriends.get(ab.msisdn).getOrElse("false") + ",true"
    })
    
    // Find users who are not a contact but are hike friends
    // Put their uid as null until we dont find a way to get their msisdn to uid mapping
    val myHikeContactsMsisdns = myHikeContacts.ab.map(_.msisdn)
    val notContactButFriendsNodes = myHikeFriends.keySet.filterNot(key => myHikeContactsMsisdns.contains(key)).map(msisdn => {
      myUid + "," + myMsisdn + "," + getUidFromMsisdn(msisdn) + "," + msisdn + ",false," + myHikeFriends.get(msisdn).get + ",true"
    }).toList
    
    contactNodes ++ notContactButFriendsNodes
  }
  
  def getUidFromMsisdn(msisdn: String): String = {
    //Fetch the uid from msisdn from redis
    val uid = MySql.getUserIdFromMsisdn(msisdn)
    println(s"Fetched uid is $uid for $msisdn")
    if(uid == null || "null".equals(uid)) {
      dumpData(errorFile, s"Fatal error. Msisdn $msisdn not found in mysql \n")
    }
    uid
  }
  
  def callAbApi(myUid: String): ABResponse = {
    println(s"Calling contact api for $myUid")
    
    val url = s"http://addressbookapi.hike.in/addressbook?uid=$myUid&ab=true&rab=false&onlyhike=true"
    println(url)
    val jsonResponse = scala.io.Source.fromURL(url).mkString
    val abResponse: ABResponse = convertJsonToObject[ABResponse](jsonResponse)
    
    if(abResponse.stat.equalsIgnoreCase("fail")) {
      // Write this failure log to a file
      dumpData(errorFile, "Empty stat == " + myUid + "\n")
    }
    if(abResponse.msisdn == null) {
      // UID not found in addressbook
      dumpData(errorFile, "No msisdn for uid == " + myUid + " in the addressbook api response\n")
    }
    abResponse
  }
  
  def callFriendsApi(myUid: String, myMsisdn: String): Map[String, String] = {
    println(s"Calling friends api for $myUid  and $myMsisdn" )
    val url = "http://addressbookapi.hike.in/v2/consoleapi/get_friends?uid=" + myUid + "&msisdn=" + myMsisdn
    println(url)
    val jsonResponse = scala.io.Source.fromURL(url).mkString
    val typeToken: Type = new TypeToken[java.util.HashMap[String, Friendship]](){}.getType();

    val friendsResponse: java.util.HashMap[String, Friendship] = new Gson().fromJson(jsonResponse, typeToken)
    
    if(friendsResponse.isEmpty || friendsResponse.size == 0) {
      // Write this failure log to a file
      dumpData(errorFile, "Empty friends list== " + myUid + " " + myMsisdn + "\n")
    }
    
    friendsResponse.filter(f => f._2.first.equals("ADDED")).map(idToFriendship => idToFriendship._1 -> "true")
  }
  
  def convertJsonToObject[T: ClassTag](jsonString: String): T = {
    try{
      new Gson().fromJson(jsonString, classTag[T].runtimeClass)
    } catch {
      case ex:Exception => {
        println("======Error occured while converting json=======" + ex.printStackTrace())
        null.asInstanceOf[T]
      }
    }
  }
  
  def dumpData(fileLocation:String, line: String) {
    val fw = new FileWriter(fileLocation, true)
    try {
      fw.write(line)
      println(s"Dumped the data to file $fileLocation")
    } catch {
      case ex:Exception => {
        println("=======ERROR========" + ex.printStackTrace())
        dumpData(errorFile, "Error while writing to file " + line + "\n")
      }
    }
    finally {
    	fw.close()       
    }
  }
  
  def getExecutorService(newNumberOfThreads: Int, oldNumberOfThreads: Int) {
    if(es == null) {
      //Start the new es with new number of threads
      es = Executors.newFixedThreadPool(newNumberOfThreads)
    }
    if (newNumberOfThreads <= 0) {
      // Shutdown the old es
      es.shutdown()
      System.exit(0)
    }
    if(newNumberOfThreads != oldNumberOfThreads) {
      // Shutdown the old es
      es.shutdown()
      //Start the new es with new number of threads
      es = Executors.newFixedThreadPool(newNumberOfThreads)
    }
  }
  
  def main(args: Array[String]) {
    println("========Staring Job..=============")
    var start = 0
    if(args.length > 0) {
      start = args(0).toInt
      println(s"Resuming from $start")
    } else {
      // Put the header line in the CSV file
      dumpData(relationshipDumpFile, csvHeader)
    }

    // Create a mysql connection
    MySql.createConnection()

    val lines = Source.fromFile(inputFilename).getLines.toList
    var oldNumberOfThreads = 0
    var newNumberOfThreads = 0
    
    while(start < lines.length) {
      // Get the new number of threads for rate limiting.
      newNumberOfThreads = getNewThreadCount
      getExecutorService(newNumberOfThreads, oldNumberOfThreads)
      var end = start + newNumberOfThreads

      println(s"=========>Starting next iteration with $newNumberOfThreads threads from $start to $end" )
      
      val listOfFutures = new ListBuffer[Future[List[String]]]()
      
      while(start < end && start < lines.length) {
        val uid = lines(start).trim
        if(uid != "") {
          println(s"Line number $start -> uid is $uid")
          try {
            // Launch a new thread here.
            listOfFutures += (launchANewThread(uid))
          } catch {
            case ex: Exception => {
              println("=======FATAL ERROR========" + ex.printStackTrace())
              dumpData(errorFile, "Failed iteration " + uid + "\n")
            }
          }
        }
        // Increment start
        start = start + 1
      }
      
      // Get the relationships from all the threads.Do only one insert
      val allRelationships = listOfFutures.map(future => future.get).flatMap(list => list).toList
      
      // Dump all the relationships in the output file only if there is something to dump
      if(allRelationships.length > 0) {
        // Dump the data to a file
        dumpData(relationshipDumpFile, allRelationships.mkString("\n") + "\n")
        // Push the current user to redis
        Redis.putUniqueKeyToRedis(List(allRelationships(0).split(",")(0) + "," + allRelationships(0).split(",")(1)))
        // Push the unique contacts to redis
        Redis.putUniqueKeyToRedis(allRelationships.map(value => value.split(",")(2) + "," + value.split(",")(3)))
      }
      
      // Update the start pointer
      start = end
      
      //Make the thread count old
      oldNumberOfThreads = newNumberOfThreads
    }

    // Dumping all the redis data to a file
    println("Dumping the redis to a file")
    dumpData(redisDumpFileLocation, Redis.getAllData())
    println("===============Job completed successfully================")
    
    // Shutdown the es
    es.shutdown()
    
    // close the connection to Mysql
    MySql.closeConnection
  }
  
}
