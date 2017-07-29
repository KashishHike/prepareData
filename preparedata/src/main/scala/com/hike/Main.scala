package com.hike

import scala.io.Source
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import com.google.gson.Gson
import java.util.concurrent.Executors
import java.util.concurrent.ExecutorService
import java.util.concurrent.Callable
import java.util.concurrent.Future
import scala.collection.mutable.ListBuffer
import java.io.FileWriter


object Main {
  
  var es: ExecutorService = null
  val prefix = "/home/kashish"
  val relationshipDumpFile = s"$prefix/relationships"
  val errorFile = s"$prefix/errors"
  val numThreadsfileName = s"$prefix/numThreads"
  val inputFilename = s"$prefix/000000_0"
  val redisDumpFileLocation = s"$prefix/redisDump"
  
  def launchANewThread(uid: String): Future[List[String]] = {
    es.submit(new Callable[List[String]]() {
        def call(): List[String] = {
          println(s"Checking for: $uid")
          return callAbApi(uid)
        }
    });
  }
  
  def getNewThreadCount : Int = {
    val newNumberOfThreads = Source.fromFile(numThreadsfileName).getLines.toList(0).trim
    if (newNumberOfThreads == null || newNumberOfThreads.trim() == "" || newNumberOfThreads.toInt <= 0) {
      println(s"Invalid number of threads $newNumberOfThreads")
    }
    newNumberOfThreads.toInt
  }
  
  def callAbApi(uid: String) :List[String] = {
    val url = "http://addressbookapi.hike.in/addressbook?uid=" + uid + "&ab=true&rab=false&onlyhike=true"
    val jsonResponse = scala.io.Source.fromURL(url).mkString
    val abResponse = convertToAbObject(jsonResponse)
    
    if(abResponse == null || abResponse.stat.equalsIgnoreCase("fail")) {
      // Write this failure log to a file
      dumpData(errorFile, "Empty stat== " + uid + "\n")

    	return List()
    }

    // Get the msisdn for this uid
    val myMsisdn = abResponse.msisdn
    println(s"Working on $uid and $myMsisdn")
    // Get the list of all the uids
    val contactRelationships = abResponse.ab.filterNot(contact => contact.msisdn.equalsIgnoreCase(myMsisdn)).map(contact => {
      myMsisdn + "," + contact.msisdn
    })
    contactRelationships.toList
  }
  
  def convertToAbObject(jsonString: String): ABResponse = {
    try{
      new Gson().fromJson(jsonString, classOf[ABResponse])
    } catch {
      case ex:Exception => {
        println("======Error occured while converting json=======" + ex.printStackTrace())
        null
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
    }

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
          // Launch a new thread here.
          listOfFutures += (launchANewThread(uid))
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
        Redis.putUniqueKeyToRedis(List(allRelationships(0).split(",")(0)))
        // Push the unique contacts to redis
        Redis.putUniqueKeyToRedis(allRelationships.map(value => value.split(",")(1)))
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
  }
  
  
  // Shutdown the es
  es.shutdown()
  
}
