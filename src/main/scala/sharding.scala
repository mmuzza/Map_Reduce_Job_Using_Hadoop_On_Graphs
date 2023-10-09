package com.lsc
import NetGraphAlgebraDefs.GraphPerturbationAlgebra.ModificationRecord
import NetGraphAlgebraDefs.NetModelAlgebra.{actionType, outputDirectory}
import NetGraphAlgebraDefs.{Action, GraphPerturbationAlgebra, NetGraph, NetGraphComponent, NetModelAlgebra, NodeObject}
import NetModelAnalyzer.Analyzer
import Randomizer.SupplierOfRandomness
import Utilz.{CreateLogger, NGSConstants}
import com.google.common.graph.{EndpointPair, ValueGraph}
import java.util.concurrent.{ThreadLocalRandom, TimeUnit}
import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext, Future}
import com.typesafe.config.ConfigFactory
import guru.nidi.graphviz.engine.Format
import org.slf4j.{Logger, LoggerFactory}
import java.io.{BufferedReader, BufferedWriter, File, FileReader, FileWriter, PrintWriter}
import java.net.{InetAddress, NetworkInterface, Socket}
import java.text.DecimalFormat
import scala.collection.mutable.CollisionProofHashMap.Node
import scala.collection.parallel.ParSeq
import scala.util.{Failure, Success}
import scala.jdk.CollectionConverters.*
import scala.io.Source

object sharding {

  val logger = LoggerFactory.getLogger(this.getClass)


  // This function is called through main via "callToExecute"
  // To create CSV FILE WITH ALL POSSIBLE COMBINATIONS Of NODES
  // We will create a csv file which will contain:
  // In column 1: original node, followed by their 8 properties
  // In column 9: perturbed node followed by their 8 properties
  // Each original node is being compared with all perturbed nodes
  def writeNodesCsvFile(filePath: String, originalNodes: java.util.Set[NodeObject], perturbedNodes: java.util.Set[NodeObject]): Unit = {

    logger.info("Creating CSV files with original and perturbed nodes via cartesian product rule")

    val writer = new BufferedWriter(new FileWriter(filePath))
    val originalNodeList = originalNodes.asScala.toList
    val perturbedNodeList = perturbedNodes.asScala.toList

    // Write the CSV header
    writer.write("originalId,originalChildren,originalProps,originalCurrentDepth,originalPropValueRange," +
      "originalMaxDepth,originalMaxBranchingFactor,originalMaxProperties,originalStoredValue," +
      "perturbedId,perturbedChildren,perturbedProps,perturbedCurrentDepth,perturbedPropValueRange," +
      "perturbedMaxDepth,perturbedMaxBranchingFactor,perturbedMaxProperties,perturbedStoredValue\n")

  // Generate all combinations of original and perturbed nodes
    val combinations = originalNodeList.flatMap { originalNode =>
      perturbedNodeList.map { perturbedNode =>
        val perturbedNodeId = Option(perturbedNode).map(_.id.toString).getOrElse("")

      // Concatenate all values into a single string
        s"${originalNode.id},${originalNode.children},${originalNode.props},${originalNode.currentDepth},${originalNode.propValueRange}," +
        s"${originalNode.maxDepth},${originalNode.maxBranchingFactor},${originalNode.maxProperties},${originalNode.storedValue}," +
        s"$perturbedNodeId,${Option(perturbedNode).map(_.children).getOrElse("")},${Option(perturbedNode).map(_.props).getOrElse("")}," +
        s"${Option(perturbedNode).map(_.currentDepth).getOrElse("")},${Option(perturbedNode).map(_.propValueRange).getOrElse("")}," +
        s"${Option(perturbedNode).map(_.maxDepth).getOrElse("")},${Option(perturbedNode).map(_.maxBranchingFactor).getOrElse("")}," +
        s"${Option(perturbedNode).map(_.maxProperties).getOrElse("")},${Option(perturbedNode).map(_.storedValue).getOrElse("")}"
      }
    }

    // Write the combinations to the file
    combinations.foreach { line =>
      writer.write(line + "\n")
    }

    writer.close()

    logger.info("CSV file was successfully created")
  }


  // Taking The CSV we created and sharding them
  // We create multiple files based on the chunksize
  // If chunk size is 10, each csv file will contain 10 rows
  // And repeat until all data has been stored successfully
  def shardGraphsForNode(inputFilePath: String, outputFolderPath: String, chunkSize: Int): Unit = {

    logger.info("Sharding the CSV file into multiple csv files")

    // Read all lines from the input file
    val lines = Source.fromFile(inputFilePath).getLines().toList

    // Split the lines into header and data
    val (header, data) = lines match {
      case h :: rest => (h, rest)
      case _ => ("", Nil)
    }

    // Group the data into chunks of size chunkSize
    val chunkedData = data.sliding(chunkSize, chunkSize).toList

    // Create and write each chunk to separate output files
    val fileStreams = chunkedData.zipWithIndex.map { case (chunk, index) =>
      val outputFilePath = s"$outputFolderPath/shard$index.csv"
      (outputFilePath, header :: chunk)
    }

    // Write each chunk to its respective output file
    fileStreams.foreach { case (outputFile, linesToWrite) =>
      val writer = new PrintWriter(new File(outputFile))
      linesToWrite.foreach(writer.println)
      writer.close()
    }

    logger.info("Sharding was Successful")
  }


  // This function is called through main via "callToExecute"
  // To create CSV FILE WITH ALL POSSIBLE COMBINATIONS Of EDGES
  // We will create a csv file which will contain:
  // In column 1: original edge, followed by their 8 properties
  // In column 9: perturbed edge followed by their 8 properties
  // Each original node is being compared with all perturbed nodes
  def writeEdgesCsvFile(filePath: String, originalEdgeList: java.util.Set[EndpointPair[NodeObject]], perturbedEdgeList: java.util.Set[EndpointPair[NodeObject]]): Unit = {

    logger.info("Creating CSV files with original and perturbed nodes via cartesian product rule")

    val writer = new BufferedWriter(new FileWriter(filePath))

    // Write the CSV header
    writer.write("origSourceId,origSourceChildren,origSourceProps,origSourceCurrentDepth,origSourcePropValueRange," +
      "origSourceMaxDepth,origSourceMaxBranchingFactor,origSourceMaxProperties,origSourceStoredValue," +
      "origVNodeId,origVNodeChildren,origVNodeProps,origVNodeCurrentDepth,origVNodePropValueRange," +
      "origVNodeMaxDepth,origVNodeMaxBranchingFactor,origVNodeMaxProperties,origVNodeStoredValue," +
      "perturbSourceId,perturbSourceChildren,perturbSourceProps,perturbSourceCurrentDepth,perturbSourcePropValueRange," +
      "perturbSourceMaxDepth,perturbSourceMaxBranchingFactor,perturbSourceMaxProperties,perturbSourceStoredValue," +
      "perturbVNodeId,perturbVNodeChildren,perturbVNodeProps,perturbVNodeCurrentDepth,perturbVNodePropValueRange," +
      "perturbVNodeMaxDepth,perturbVNodeMaxBranchingFactor,perturbVNodeMaxProperties,perturbVNodeStoredValue\n"
    )


    originalEdgeList.forEach { originalEdge =>
      perturbedEdgeList.forEach { perturbedEdge =>

        val csvLine = s"${originalEdge.source().id}, ${originalEdge.source().children}, ${originalEdge.source().props}," +
          s"${originalEdge.source().currentDepth}, ${originalEdge.source().propValueRange}, ${originalEdge.source().maxDepth}, " +
          s"${originalEdge.source().maxBranchingFactor}, ${originalEdge.source().maxProperties}, ${originalEdge.source().storedValue}," +
          s"${originalEdge.nodeV().id}, ${originalEdge.nodeV().children}, ${originalEdge.nodeV().props}," +
          s"${originalEdge.nodeV().currentDepth}, ${originalEdge.nodeV().propValueRange}, ${originalEdge.nodeV().maxDepth}, " +
          s"${originalEdge.nodeV().maxBranchingFactor}, ${originalEdge.nodeV().maxProperties}, ${originalEdge.nodeV().storedValue}," +
          s"${perturbedEdge.source().id}, ${perturbedEdge.source().children}, ${perturbedEdge.source().props}," +
          s"${perturbedEdge.source().currentDepth}, ${perturbedEdge.source().propValueRange}, ${perturbedEdge.source().maxDepth}, " +
          s"${perturbedEdge.source().maxBranchingFactor}, ${perturbedEdge.source().maxProperties}, ${perturbedEdge.source().storedValue}," +
          s"${perturbedEdge.nodeV().id}, ${perturbedEdge.nodeV().children}, ${perturbedEdge.nodeV().props}," +
          s"${perturbedEdge.nodeV().currentDepth}, ${perturbedEdge.nodeV().propValueRange}, ${perturbedEdge.nodeV().maxDepth}, " +
          s"${perturbedEdge.nodeV().maxBranchingFactor}, ${perturbedEdge.nodeV().maxProperties}, ${perturbedEdge.nodeV().storedValue}\n"

        writer.write(csvLine)
      }
    }
    writer.close()

    logger.info("CSV file was successfully created")
  }


  // This function is called by "Main" class
  // This function executes this entire class by calling the functions
  // It first stores the ngs file and gets the list of nodes from original and perturbed
  // Using the list of nodes it calls a function to create a csv file with original and perturbed nodes (Cartesian product)
  // Using the list of edges it calls a function to create a csv file with original and perturbed edges (Cartesian product)
  // It takes both edges and nodes csv files and call sharding function which shards each csv file and stores them
  // Arguments are set inside of "application.conf"... Please change the arguments over there for code to compile
  // And properly produce results
  def callToExecute(args: String*): Unit = {

    logger.info("Main function has called for sharding files for original and perturbed graph")
    logger.info("The process has begun")

    logger.info("Loading in original graph ngs and perturbed graph ngs files using NetGraph.load function:")
    val originalGraph: Option[NetGraph] = NetGraph.load({args(6)}, {args(7)})
    val perturbedGraph: Option[NetGraph] = NetGraph.load({args(8)}, {args(9)})

    logger.info("Gathering information of the graphs")
    val netOriginalGraph: NetGraph = originalGraph.get // getiting original graph info
    val netPerturbedGraph: NetGraph = perturbedGraph.get // getiting perturbed graph info

    logger.info("Storing nodes for in a list for original graph")
    val originalGraphNodes: java.util.Set[NodeObject] = netOriginalGraph.sm.nodes()

    logger.info("Storing nodes for in a list for perturbed graph")
    val perturbedGraphNodes: java.util.Set[NodeObject] = netPerturbedGraph.sm.nodes()

    logger.info("Storing The Edges in a list for both Original Graph and the Perturbed Graph")
    val originalGraphEdges: java.util.Set[EndpointPair[NodeObject]] = netOriginalGraph.sm.edges()
    val originalEdgeList = originalGraphEdges.asScala.toList
    val perturbedGraphEdges: java.util.Set[EndpointPair[NodeObject]] = netPerturbedGraph.sm.edges()
    val perturbedEdgeList = perturbedGraphEdges.asScala.toList

    // Below are all the function call that are used in this class to do sharding of the files
    logger.info("Creating a CSV file with combination of each node of original with all nodes in perturbed")
    // Args 0 represents the file path to creating csv with combination of O x P graph nodes
    writeNodesCsvFile({args(0)}, originalGraphNodes, perturbedGraphNodes)

    logger.info("Creating a CSV file with combination of each edge of original with all edges in perturbed")
    // Args 0 represents the file path to creating csv with combination of O x P graph edges
    writeEdgesCsvFile({args(3)}, originalGraphEdges, perturbedGraphEdges)

    logger.info("Sharding the Nodes CSV files into 10's")
    // Args 1 = input of the csv file with cartesian product of O x P graph nodes
    // Args 2 = output directory of storing all the sharded nodes of O x P graph nodes
    shardGraphsForNode({args(1)}, {args(2)}, 10)

    logger.info("Sharding the Edges CSV files into 10's")
    // Args 1 = input of the csv file with cartesian product of O x P graph edges
    // Args 2 = output directory of storing all the sharded nodes of O x P graph edges
    shardGraphsForNode({args(4)}, {args(5)}, 10)

    logger.info("Sharding has completed successfully")
  }

}

