package com.lsc
import NetGraphAlgebraDefs.NetModelAlgebra.logger
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.spi.LoggerFactory
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{DoubleWritable, IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred.{FileInputFormat, FileOutputFormat, JobClient, JobConf, TextInputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

import java.text.DecimalFormat
import scala.collection.JavaConverters.*

object nodesMapReduce {

  // Sim Rank algorithm compares the Nodes properties of original with the perturbed
  // All properties are used excluding the Node "ID"
  // It then generates a score determining whether it was removed, modified, or added based on the threshold.
  // The threshold for match is set to 0.9, anything above is modified, and below is considered removed
  def calculateSimRank(csvLine: String): Double = {

    println(csvLine)

    // Split the CSV line by comma to extract fields
    val fields = csvLine.split(",")

    var score: Double = 0.0

    // val originalId = fields(0).trim.toDouble
    val originalChildren = fields(1).trim.toDouble
    val originalProps = fields(2).trim.toDouble
    val originalCurrentDepth = fields(3).trim.toDouble
    val originalPropValueRange = fields(4).trim.toDouble
    val originalMaxDepth = fields(5).trim.toDouble
    val originalMaxBranchingFactor = fields(6).trim.toDouble
    val originalMaxProperties = fields(7).trim.toDouble
    val originalStoredValue = fields(8).trim.toDouble

    // val perturbedId = fields(9).trim.toDouble
    val perturbedChildren = fields(10).trim.toDouble
    val perturbedProps = fields(11).trim.toDouble
    val perturbedCurrentDepth = fields(12).trim.toDouble
    val perturbedPropValueRange = fields(13).trim.toDouble
    val perturbedMaxDepth = fields(14).trim.toDouble
    val perturbedMaxBranchingFactor = fields(15).trim.toDouble
    val perturbedMaxProperties = fields(16).trim.toDouble
    val perturbedStoredValue = fields(17).trim.toDouble

    if (originalChildren == perturbedChildren) {
      score += 0.1
    }
    else if (originalChildren < perturbedChildren) {
      score += 0.02
    }
    else {
      score += 0.02
    }


    if (originalProps == perturbedProps) {
      score += 0.1
    }
    else if (originalProps < perturbedProps) {
      score += 0.02
    }
    else {
      score += 0.02
    }


    if (originalCurrentDepth == perturbedCurrentDepth) {
      score += 0.1
    }
    else if (originalCurrentDepth < perturbedCurrentDepth) {
      score += 0.02
    }
    else {
      score += 0.02
    }


    if (originalPropValueRange == perturbedPropValueRange) {
      score += 0.2
    }
    else if (originalPropValueRange < perturbedPropValueRange) {
      score += 0.1
    }
    else {
      score += 0.2
    }


    if (originalMaxDepth == perturbedMaxDepth) {
      score += 0.1
    }
    else if (originalMaxDepth < perturbedMaxDepth) {
      score += 0.02
    }
    else {
      score += 0.02
    }

    if (originalMaxBranchingFactor == perturbedMaxBranchingFactor) {
      score += 0.1
    }
    else if (originalMaxBranchingFactor < perturbedMaxBranchingFactor) {
      score += 0.2
    }
    else {
      score += 0.2
    }


    if (originalMaxProperties == perturbedMaxProperties) {
      score += 0.1
    }
    else if (originalMaxProperties < perturbedMaxProperties) {
      score += 0.02
    }
    else {
      score += 0.02
    }


    if (originalStoredValue == perturbedStoredValue) {
      score += 0.1
    }
    else if (originalStoredValue < perturbedStoredValue) {
      score += 0.2
    }
    else {
      score += 0.2
    }

    val df = new DecimalFormat("#.##") // Format to two decimal places

    df.format(score).toDouble
  }


  // This is the Mapper class which defines the map function inside of it
  // It takes 4 things: KeyIn, valueIn, KeyOut, ValOut
  // KeyIn and ValIn is used my the map function which it takes automatically line by line
  // ValueIn is the line being read in the csv
  // We manipulate the ValueIn and send it to simRank to calculate score
  // Score received by the simRank is set to ValueOut for the reducer
  // KeyOut is set to the original node which is index 0 of csv line (ValueIn)
  class MyMapper extends Mapper[LongWritable, Text, Text, DoubleWritable] { // KeyIn, ValIn, KeyOut, ValOut

    private val node = new Text() // Key out
    private val score = new DoubleWritable // Value out

    override def map(
                      key: LongWritable,
                      value: Text,
                      context: Mapper[LongWritable, Text, Text, DoubleWritable]#Context
                    ): Unit = {

      //logger.info("Map function is Being Executed")

      val lineTracker = key.get()

      if (lineTracker > 0) {
        val line = value.toString
        val fields = line.split(",") // Split the key in for mapper with ","

        //logger.info("Calculating SimRank score for perturbed and original nodes")
        node.set(fields(0))
        val calculatedScore = calculateSimRank(value.toString)
        score.set(calculatedScore) // Value Out for reducer

        //logger.info("Sending Original Graph Node as key & SimRank Score Between Two Nodes As The Value To Reducer")
        context.write(node, score)
      }
    }
  } // end of mapper

  // Reducer takes in 4 arguments
  // First is the KeyIn and ValueIn it receives from the mapper
  // KeyIn will be the original Node and ValueIn will be the simRank score
  // In my code below reducer picks the score that all: meet threshold of 0.9, below and above
  // It stores it in a big string with all the information and is set as the ValueOut
  class MyReducer extends Reducer[Text, DoubleWritable, Text, Text] {
    override def reduce(
                         key: Text,
                         values: java.lang.Iterable[DoubleWritable],
                         context: Reducer[Text, DoubleWritable, Text, Text]#Context
                       ): Unit = {


      logger.info("Reduce Function is Being Executed")

      val scores = values.asScala.map(_.get()) // Extract Double values

      logger.info("Calculating the Number of Nodes compared with Original Node exceeded the Threshold indicating Modification")
      val greaterThanCount = scores.count(_ > 0.9)
      logger.info("Calculating If Any Score from SimRank Matched the 0.9 Threshold Indicating Node was Found")
      val equalToCount = scores.count(_ == 0.9)
      logger.info("Calculating the Number of Nodes Compared with Original Node were under the Threshold indicating Removed")
      val lessThanCount = scores.count(_ < 0.9)


      val ctl = scores.count(_ > 0.8) // Correct Traceability Links
      val wtl = scores.count(_ < 0.8) // Wrong Traceability Links
      val dtl = scores.count(_ < 0.9) // Discarded Traceability Links
      val atl = equalToCount + greaterThanCount // Accepted Traceability Links

      val btl = ctl + wtl // traceability links that may be incorrect (ctl + wtl)
      val gtl = dtl + atl // traceability links that are correct (dtl + atl)
      val rtl = btl + gtl // total number of traceability links (btl + rtl)

      val accRation = 0 // ratio that measures the accuracy of your algorithm
      val vprRation = 0 // ratio used to evaluate the precision of your algorithm

      var info = ""

      if (equalToCount > 0) {
        info = s"Node $key: Matched in Perturbed Graph"
      }
      else if (greaterThanCount > 0) {
        info = s"Node $key: Modified in Perturbed Graph"
      }
      else {
        info = s"Node $key: Removed in Perturbed Graph"
      }


      logger.info("Outputting Information to a Csv File")
      val outputMessage = s"\n$info \nBTL: $btl \nGTL: $gtl \nRTL: $rtl \nCTL: $ctl \nWTL: $wtl \nDTL: $dtl \nATL: $atl\n\n"

      logger.info("Writing each unique key with its value to a csv file")
      context.write(key, new Text(outputMessage))
    }
  } // end of reducer


  def executeMapReduceJobForNodes(args: String*): Unit = {

    logger.info("Creating a Hadoop configuration")
    val configuration = new Configuration()

    logger.info("Setting input and output paths (hard coding)")
    val inputPath = new Path({args(0)})
    val outputPath = new Path({args(1)})

    logger.info("Create a Hadoop job instance with a name")
    val job = Job.getInstance(configuration, "MyMapReduceJob")

    logger.info("Setting the JAR file containing the driver class")
    //     job.setJarByClass(MyMapReduceApp.getClass)

    logger.info("Set Mapper and Reducer classes")
    job.setMapperClass(classOf[MyMapper])
    job.setReducerClass(classOf[MyReducer])

    logger.info("Set Mapper and Reducer output key-value types")
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[DoubleWritable])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])

    logger.info("Set input and output paths")
    org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, inputPath)
    org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job, outputPath)

    logger.info("Submitting the job and waiting for completion")
    if (job.waitForCompletion(true)) {
      logger.info("Job completed successfully!")
      //System.exit(1)
    } else {
      logger.info("Job failed!")
      System.exit(0)
    }
  }
  
  
} // end of map reduce class


