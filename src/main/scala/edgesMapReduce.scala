
package com.lsc
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{DoubleWritable, IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred.{FileInputFormat, FileOutputFormat, JobClient, JobConf}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.slf4j.LoggerFactory

import java.text.DecimalFormat
import scala.collection.JavaConverters.*

object edgesMapReduce {

  private val logger = LoggerFactory.getLogger(this.getClass)

  // Sim Rank algorithm compares the edges properties of original with the perturbed
  // It first checks using the 8 properties excluding the ID to see if source node of both edges match
  // If source node is equivalent and there is a match then it goes and looks and compared the V NOde properties
  // V Node is the node it connects to at the opposite end (I noticed that when i printed the edge out)
  // It the generates a score determining whether it was removed, modified, or added.
  // Honestly this algorithm is complete trash, Struggled with understanding what was needed to be used
  // Discovered the NetModelAnalyzer later on, believed we must use that but it was too late
  // The score generated from this algorithm for edges is not accurate
  def simRank(csvLine: String): Double = {

    logger.info("Calculating Score")

    var score: Double = 0.0

    // Split the CSV line by comma to extract fields
    val fields = csvLine.split(",")

    logger.info("Checking to see If Source Node Matches")
    
    // Children
    if (fields(1).trim.toDouble == fields(19).trim.toDouble ) {
      score += 0.1
    }
    else if (fields(1).trim.toDouble < fields(19).trim.toDouble) {
      score += 0.02
    }
    else {
      score += 0.02
    }

    // Props
    if (fields(2).trim.toDouble == fields(20).trim.toDouble) {
      score += 0.1
    }
    else if (fields(2).trim.toDouble < fields(20).trim.toDouble) {
      score += 0.02
    }
    else {
      score += 0.02
    }

    // Current Depth
    if (fields(3).trim.toDouble == fields(21).trim.toDouble) {
      score += 0.1
    }
    else if (fields(3).trim.toDouble < fields(21).trim.toDouble) {
      score += 0.02
    }
    else {
      score += 0.02
    }

    // Prop Value Range
    if (fields(4).trim.toDouble == fields(22).trim.toDouble) {
      score += 0.2
    }
    else if (fields(4).trim.toDouble < fields(22).trim.toDouble) {
      score += 0.1
    }
    else {
      score += 0.2
    }

    // Max Depth
    if (fields(5).trim.toDouble == fields(23).trim.toDouble) {
      score += 0.1
    }
    else if (fields(5).trim.toDouble < fields(23).trim.toDouble) {
      score += 0.02
    }
    else {
      score += 0.02
    }

    // Max Branching Factor
    if (fields(6).trim.toDouble == fields(24).trim.toDouble) {
      score += 0.1
    }
    else if (fields(6).trim.toDouble < fields(24).trim.toDouble) {
      score += 0.2
    }
    else {
      score += 0.2
    }

    // Max Properties
    if (fields(7).trim.toDouble == fields(25).trim.toDouble) {
      score += 0.1
    }
    else if (fields(7).trim.toDouble < fields(25).trim.toDouble) {
      score += 0.02
    }
    else {
      score += 0.02
    }

    // Stored Value
    if (fields(8).trim.toDouble == fields(26).trim.toDouble) {
      score += 0.1
    }
    else if (fields(8).trim.toDouble < fields(26).trim.toDouble) {
      score += 0.2
    }
    else {
      score += 0.2
    }

    val df = new DecimalFormat("#.##") // Format to two decimal places

    // If source node matches then we check V Node to see if it has been modified
    if(df.format(score).toDouble == 0.9){

      logger.info("Source Node has a match")
      logger.info("Checking V Node Properties of each edge")

      score = 0.0;

      // Children
      if (fields(10).trim.toDouble == fields(28).trim.toDouble) {
        score += 0.1
      }
      else if (fields(10).trim.toDouble < fields(28).trim.toDouble) {
        score += 0.02
      }
      else {
        score += 0.02
      }

      // Props
      if (fields(11).trim.toDouble == fields(29).trim.toDouble) {
        score += 0.1
      }
      else if (fields(11).trim.toDouble < fields(29).trim.toDouble) {
        score += 0.02
      }
      else {
        score += 0.02
      }

      // Current Depth
      if (fields(12).trim.toDouble == fields(30).trim.toDouble) {
        score += 0.1
      }
      else if (fields(12).trim.toDouble < fields(30).trim.toDouble) {
        score += 0.02
      }
      else {
        score += 0.02
      }

      // Prop Value Range
      if (fields(13).trim.toDouble == fields(31).trim.toDouble) {
        score += 0.2
      }
      else if (fields(13).trim.toDouble < fields(31).trim.toDouble) {
        score += 0.1
      }
      else {
        score += 0.2
      }

      // Max Depth
      if (fields(14).trim.toDouble == fields(32).trim.toDouble) {
        score += 0.1
      }
      else if (fields(14).trim.toDouble < fields(32).trim.toDouble) {
        score += 0.02
      }
      else {
        score += 0.02
      }

      // Max Branching Factor
      if (fields(15).trim.toDouble == fields(33).trim.toDouble) {
        score += 0.1
      }
      else if (fields(15).trim.toDouble < fields(33).trim.toDouble) {
        score += 0.2
      }
      else {
        score += 0.2
      }

      // Max Properties
      if (fields(16).trim.toDouble == fields(34).trim.toDouble) {
        score += 0.1
      }
      else if (fields(16).trim.toDouble < fields(34).trim.toDouble) {
        score += 0.02
      }
      else {
        score += 0.02
      }

      // Stored Value
      if (fields(17).trim.toDouble == fields(35).trim.toDouble) {
        score += 0.1
      }
      else if (fields(17).trim.toDouble < fields(35).trim.toDouble) {
        score += 0.2
      }
      else {
        score += 0.2
      }
    }

    logger.info("Score is Generated")
    score
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
    private val header = new Text()
    private val score = new DoubleWritable // Value out

    override def map(
                      key: LongWritable,
                      value: Text,
                      context: Mapper[LongWritable, Text, Text, DoubleWritable]#Context
                    ): Unit = {

      logger.info("Map function is Being Executed")

      val lineTracker = key.get()

      logger.info("Skipping the header in the CSV file being read")
      if (lineTracker > 0) {
        val line = value.toString
        val fields = line.split(",") // Split the key in for mapper with ","

        logger.info("Calculating SimRank score for perturbed and original edges")
        node.set(fields(0) + "-->" + fields(9))
        val calculatedScore = simRank(value.toString)
        score.set(calculatedScore) // Value Out for reducer

        logger.info("Sending Original Graph edges as key & SimRank Score Between Two Edges As The Value To Reducer")
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

      logger.info("Calculating the Number of Edges compared with Original Edges exceeded the Threshold indicating Modification")
      val greaterThanCount = scores.count(_ > 0.9)
      logger.info("Calculating If Any Score from SimRank Matched the 0.9 Threshold Indicating Edge was Found")
      val equalToCount = scores.count(_ == 0.9)
      logger.info("Calculating the Number of Edges Compared with Original Edges were under the Threshold indicating Removed")
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
        info = s"Matched in Perturbed Graph"
      }
      else if (greaterThanCount > 0) {
        info = s"Modified in Perturbed Graph"
      }
      else {
        info = s"Removed in Perturbed Graph"
      }

      logger.info("Outputting Information to a Csv File")
      val outputMessage = s"$info, $btl, $gtl, $rtl, $ctl, $wtl, $dtl, $atl\n"

      logger.info("Writing each unique key with its value to a csv file")
      context.write(key, new Text(outputMessage))
    }
  } // end of reducer


  def main(args: Array[String]): Unit = {

    logger.info("Creating a Hadoop configuration")
    val configuration = new Configuration()

    logger.info("Setting input and output paths (hard coding)")
//    val inputPath = new Path("/Users/muzza/desktop/CS440/shardedFileEdges/shard25177.csv")
    val inputPath = new Path("/Users/muzza/desktop/CS440/originalPerturbedEdges/combinedEdges.csv")
    val outputPath = new Path("/Users/muzza/desktop/CS440/reducerForEdges")

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
      System.exit(1)
    } else {
      logger.info("Job failed!")
      System.exit(0)
    }
  }

}// end of map reduce class
