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
  val relationshipDumpFile = "/Users/kashish/Desktop/relationships"
  val errorFile = "/Users/kashish/Desktop/errors"
  val numThreadsfileName = "/Users/kashish/Desktop/numThreads"
  val inputFilename = "/Users/kashish/Desktop/000000_0"
  
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
      System.exit(-1)
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
      println("Dumped the data to file")
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
  
  def main(args: Array[String]) {
    println("========Staring Job..=============")
    
    var start = 0
    if(args.length > 0) {
      start = args(0).toInt
      println(s"Resuming from $start")
    }

    val lines = Source.fromFile(inputFilename).getLines.toList
    
    while(start < lines.length) {
      // Get the new env variable here.
      val newNumberOfThreads = getNewThreadCount
      es = Executors.newFixedThreadPool(newNumberOfThreads)
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
      if(allRelationships.length > 0)
        dumpData(relationshipDumpFile, allRelationships.mkString("\n") + "\n")
      
      // Update the start pointer
      start = end
    }

    println("===============Job completed successfully================")
  }
  
}
