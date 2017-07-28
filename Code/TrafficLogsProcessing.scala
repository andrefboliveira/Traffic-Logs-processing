package pt.ulisboa.ciencias.di.cloud_computing.project3

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.joda.time.DateTime
import java.io.File
import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import scala.Ordering
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkContext
import org.apache.commons.io.FileUtils

object TrafficLogsProcessing {
  def main(args: Array[String]) {

    var inputFile = ""
    try {
      inputFile = args(0)

    } catch {
      case t: ArrayIndexOutOfBoundsException => {
        println("Provide input file")
        System.exit(0)
      }
    }

    var outputDirectory = ""
    try {
      outputDirectory = args(1)

    } catch {
      case t: ArrayIndexOutOfBoundsException => {
        outputDirectory = new File(inputFile).getParentFile.toString() + "/results"
        FileUtils.deleteDirectory(new File(outputDirectory))
        println(s"Output directory not provided! Its was used the same as input: $outputDirectory")
      }
    }

    val appName = "Project 3 - Spark: Process traffic logs"
    val conf = new SparkConf().setMaster("local").setAppName(appName)
    val sc = new SparkContext(conf)

    val csv = sc.textFile(inputFile)
    val headerAndRows = csv.map(_.split(",").map(_.trim))
    val header = headerAndRows.first

    val data = headerAndRows.mapPartitionsWithIndex((idx, iter) => if (idx == 0) iter.drop(1) else iter)
    data.cache()

    val headerList = header.toList
    val sourceIndex = headerList.indexOf("sIP")
    val destinationIndex = headerList.indexOf("dIP")
    val protocolIndex = headerList.indexOf("protocol")
    val packetsIndex = headerList.indexOf("packets")
    val bytesIndex = headerList.indexOf("bytes")
    val startTimeIndex = headerList.indexOf("sTime")
    val endTimeIndex = headerList.indexOf("eTime")

    def convertDate(x: Double): DateTime = {
      return new DateTime(x.toLong * 1000).toDateTime
    }

    def getDay(x: Double): String = {
      return convertDate(x).toString("yyyy-MM-dd")
    }

    /*
		 * 
		 * Question 1
		 * Total Information Sent
		 * 
		 */

    val pairsInfoSent = data.map(line => (line(sourceIndex), line(bytesIndex).toLong))

    val sumInfoByPair = pairsInfoSent.reduceByKey((a, b) => a + b)

    val maxInfo = sumInfoByPair.max()(new Ordering[Tuple2[String, Long]]() {
      override def compare(x: (String, Long), y: (String, Long)): Int =
        Ordering[Long].compare(x._2, y._2)
    })
    sc.parallelize(Array(maxInfo)).saveAsTextFile(outputDirectory + "/Question_1_total/")
    println(s"QUESTION 1 (total) - Higher total information sent from sIP: ${maxInfo._1} (with ${maxInfo._2} bytes)")

    /*
				 * 
				 * Question 1
				 * Average per day
				 *  
				 */

    val pairsInfoSentDay = data.map(line => ((line(sourceIndex), getDay(line(startTimeIndex).toDouble)), line(bytesIndex).toLong))
    val sumInfoByPairDay = pairsInfoSentDay.reduceByKey((a, b) => a + b).map(x => (x._1._1, (x._2, 1)))

    val avgInfoByPairDay = sumInfoByPairDay.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues(y => 1.0 * y._1 / y._2)

    val maxInfoDay = avgInfoByPairDay.max()(new Ordering[Tuple2[String, Double]]() {
      override def compare(x: (String, Double), y: (String, Double)): Int =
        Ordering[Double].compare(x._2, y._2)
    })

    sc.parallelize(Array(maxInfoDay)).saveAsTextFile(outputDirectory + "/Question_1_average_per_day/")

    println(s"QUESTION 1 (average per day) - Higher average information sent from sIP: ${maxInfoDay._1} (with a average of ${maxInfoDay._2} bytes)")

    /*
				 * 
				 * Question 2
				 * 
				 *  
				 */

    val pairsCommunicateInformation = data.map(line => (
      (line(sourceIndex), line(destinationIndex)), (line(bytesIndex).toLong)))

    val sumInformationExchanged = pairsCommunicateInformation.reduceByKey((a, b) => a + b).map(x => ((x._1._1), (x._1._2, x._2)))

    sumInformationExchanged.saveAsTextFile(outputDirectory + "/Question_2/")

    println("QUESTION 2:\n" +
      "(sIP,[(dIP, bytes)])\n" +
      sumInformationExchanged.groupByKey().mapValues(_.map(y => (y._1, y._2)).mkString("[", ",", "]")).collect().mkString(",\n"))

    /*
				 * 
				 * Question 3
				 * 
				 *  
				 */

    val pairsCommunicate = data.map(line => (
      (line(sourceIndex), line(destinationIndex), getDay(line(startTimeIndex).toDouble)), (1)))

    val pairsCommunicateDay = pairsCommunicate.reduceByKey((a, b) => a + b).map(x => ((x._1._1, x._1._2), (x._2, 1)))

    val avgPairsCommunicateDay = pairsCommunicateDay.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues(y => 1.0 * y._1 / y._2).map(x => ((x._1._1, x._1._2), x._2))

    avgPairsCommunicateDay.saveAsTextFile(outputDirectory + "/Question_3/")

    println("QUESTION 3:\n" +
      "((sIP, dIP), flowsPerDay)\n" +
      avgPairsCommunicateDay.collect().mkString(",\n"))

    /*
				 * 
				 * Question 4
				 * 
				 *  
				 */

    val pairsDistinctDestinations = data.map(line => (line(sourceIndex), line(destinationIndex)))
      .distinct().map(x => (x._1, 1))

    val pairsNumberDestinations = pairsDistinctDestinations.reduceByKey((a, b) => a + b)

    val maxDestinations = pairsNumberDestinations.max()(new Ordering[Tuple2[String, Int]]() {
      override def compare(x: (String, Int), y: (String, Int)): Int =
        Ordering[Int].compare(x._2, y._2)
    })

    sc.parallelize(Array(maxDestinations)).saveAsTextFile(outputDirectory + "/Question_4/")

    println(s"QUESTION 4 - The sIP: ${maxDestinations._1}, communicates with more destinations than the remaining (communicates with ${maxDestinations._2} destinations)")

    /*
				 * 
				 * Question 5
				 * Sent more information (repetition of the first question)
				 *  
				 */

    //	val pairsInformationSent = data.map(line => (line(sourceIndex), line(bytesIndex).toLong))
    //
    //	val sumInformationSent = pairsInformationSent.reduceByKey((a, b) => a + b)
    //
    //	val maxInformationSent = sumInformationSent.max()(new Ordering[Tuple2[String, Long]]() {
    //	  override def compare(x: (String, Long), y: (String, Long)): Int =
    //		Ordering[Long].compare(x._2, y._2)
    //	})
    //	sc.parallelize(Array(maxInformationSent)).saveAsTextFile(outputDirectory + "/Question_5_sends/")
    //
    //	println(s"QUESTION 5 (sends) - The sIP that sent more information was: ${maxInformationSent._1} (with ${maxInformationSent._2} bytes sent)")

    /*
				 * 
				 * Question 5
				 * Received more information
				 *  
				 */

    val pairsInformationReceived = data.map(line => (line(destinationIndex), line(bytesIndex).toLong))

    val sumInformationReceived = pairsInformationReceived.reduceByKey((a, b) => a + b)

    val maxInformationReceived = sumInformationReceived.max()(new Ordering[Tuple2[String, Long]]() {
      override def compare(x: (String, Long), y: (String, Long)): Int =
        Ordering[Long].compare(x._2, y._2)
    })

    sc.parallelize(Array(maxInformationReceived)).saveAsTextFile(outputDirectory + "/Question_5_receives/")

    println(s"QUESTION 5 (receives) - The dIP that received more information was: ${maxInformationReceived._1} (with ${maxInformationReceived._2} bytes received)")

    /*
				 * 
				 * Question 6
				 * RDD
				 *  
				 */

    def checkRetransmission(row: (String, ((Int, Int, Long, Double), (Int, Int, Long, Double)))): Boolean = {
      //	def checkRetransmission(row: Tuple2[String, Tuple2[Tuple4[Int, Int, Long, Double], Tuple4[Int, Int, Long, Double]]]): Boolean = {
      val (_, value) = row
      val (firstValue, secondValue) = value
      return if (firstValue._1 == secondValue._1 && firstValue._2 == secondValue._2 && firstValue._3 == secondValue._3
        && (secondValue._4 - firstValue._4 < 5)) true else false
    }

    val destinationRDD = data.map(line => (
      line(destinationIndex), (line(protocolIndex).toInt, line(packetsIndex).toInt, line(bytesIndex).toLong, line(endTimeIndex).toDouble)))

    val sourceRDD = data.map(line => (
      line(sourceIndex), (line(protocolIndex).toInt, line(packetsIndex).toInt, line(bytesIndex).toLong, line(startTimeIndex).toDouble)))

    val joinRDD = destinationRDD.join(sourceRDD)
    joinRDD.persist(StorageLevel.MEMORY_AND_DISK)

    val resultRDD = joinRDD.filter(x => checkRetransmission(x)).map(_._1).distinct()

    resultRDD.saveAsTextFile(outputDirectory + "/Question_6/")

    println("QUESTION 6:\n" +
      resultRDD.collect().mkString(",\n"))

    /*
				 * 
				 * Question 6
				 * Spark SQL
				 *  
				 */

    val sparkSession = SparkSession.builder
      .config(conf = conf)
      .appName(appName + " session")
      .getOrCreate()

    val dfLogs = sparkSession.read
      .format("csv")
      //	  .format("com.databricks.spark.csv")
      .option("header", "true")
      //	  .load(inputFile)
      .csv(inputFile)

    //	dfLogs.show()

    dfLogs.createOrReplaceTempView("logs")

    val resultDF = sparkSession.sql(
      "SELECT DISTINCT l2.sIP"
        + " FROM logs AS l1, logs AS l2"
        + " WHERE l1.dIP = l2.sIP AND l1.bytes= l2.bytes AND l1.protocol = l2.protocol AND l1.packets = l2.packets"
        + " AND l2.sTime - l1.eTime < 5")

    val rowsResult: RDD[Row] = resultDF.rdd

    rowsResult.saveAsTextFile(outputDirectory + "/Question_6_alternative/")

    println("QUESTION 6 (alternative):\n" +
      rowsResult.collect().mkString(",\n"))

    sc.stop()

  }
}