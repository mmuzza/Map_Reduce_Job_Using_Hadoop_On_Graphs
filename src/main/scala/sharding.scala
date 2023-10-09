package com.lsc

import NetGraphAlgebraDefs.GraphPerturbationAlgebra.ModificationRecord
import NetGraphAlgebraDefs.NetModelAlgebra.{actionType, outputDirectory}
import NetGraphAlgebraDefs.{Action, GraphPerturbationAlgebra, NetGraph, NetGraphComponent, NetModelAlgebra, NodeObject}
import NetModelAnalyzer.Analyzer
import Randomizer.SupplierOfRandomness
import Utilz.{CreateLogger, NGSConstants}
import com.google.common.graph.{EndpointPair, ValueGraph}
//import com.lsc.sharding.{originalGraphEdges, originalGraphNodes, perturbedGraphEdges, perturbedGraphNodes, shardGraphsForNode, writeEdgesCsvFile, writeNodesCsvFile}

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

//  logger.info("Loading in original graph ngs and perturbed graph ngs files using NetGraph.load function:")
//  val originalGraph: Option[NetGraph] = NetGraph.load("NetGameSimhomework.ngs", "/Users/muzza/desktop/CS440/")
//  val perturbedGraph: Option[NetGraph] = NetGraph.load("NetGameSimhomework.ngs.perturbed", "/Users/muzza/desktop/CS440/")
//
//  logger.info("Gathering information of the graphs")
//  val netOriginalGraph: NetGraph = originalGraph.get // getiting original graph info
//  val netPerturbedGraph: NetGraph = perturbedGraph.get // getiting perturbed graph info
//
//  logger.info("Storing nodes for in a list for original graph")
//  val originalGraphNodes: java.util.Set[NodeObject] = netOriginalGraph.sm.nodes()
//  val originalNodeList = originalGraphNodes.asScala.toList
//
//  logger.info("Storing nodes for in a list for perturbed graph")
//  val perturbedGraphNodes: java.util.Set[NodeObject] = netPerturbedGraph.sm.nodes()
//  val perturbedNodeList = perturbedGraphNodes.asScala.toList


  // -------------------------------------------------------------------------

  // MAKING CSV FILES WITH ALL POSSIBLE COMBINATIONS Of NODES

  def writeNodesCsvFile(filePath: String, originalNodes: java.util.Set[NodeObject], perturbedNodes: java.util.Set[NodeObject]): Unit = {

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
  }

//  val filePath = "/Users/muzza/desktop/CS440/originalXperturbed/combinations.csv"
//  writeNodesCsvFile(filePath, originalGraphNodes, perturbedGraphNodes)





  // --------------------------------------------------------------------------

  logger.info("Shards the graphs into many csv files for mapper/reducer to later process")

//  def shardGraphsForNode(inputFilePath: String, outputFolderPath: String, chunkSize: Int): Unit = {
//    val source = Source.fromFile(inputFilePath)
//    val lines = source.getLines().toList
//    source.close()
//
//    val header = lines.head
//    val data = lines.tail
//
//    val chunkedData = data.grouped(chunkSize).toList
//
//    for ((chunk, index) <- chunkedData.zipWithIndex) {
//      val outputFilePath = s"$outputFolderPath/shard$index.csv"
//      val writer = new PrintWriter(new File(outputFilePath))
//      writer.println(header)
//      chunk.foreach(writer.println)
//      writer.close()
//    }
//  }

  def shardGraphsForNode(inputFilePath: String, outputFolderPath: String, chunkSize: Int): Unit = {

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
  }



  // --------------------------------------------------------------------------


  // --------------------------------------------------------------------------


//  logger.info("Storing The Edges in a list for both Original Graph and the Perturbed Graph")
//  val originalGraphEdges: java.util.Set[EndpointPair[NodeObject]] = netOriginalGraph.sm.edges()
//  val originalEdgeList = originalGraphEdges.asScala.toList
//  val perturbedGraphEdges: java.util.Set[EndpointPair[NodeObject]] = netPerturbedGraph.sm.edges()
//  val perturbedEdgeList = perturbedGraphEdges.asScala.toList



  // MAKING CSV FILES WITH ALL POSSIBLE COMBINATIONS

  def writeEdgesCsvFile(filePath: String, originalEdgeList: java.util.Set[EndpointPair[NodeObject]], perturbedEdgeList: java.util.Set[EndpointPair[NodeObject]]): Unit = {

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
  }


  def callToExecute(args: String*): Unit = {

    logger.info("Loading in original graph ngs and perturbed graph ngs files using NetGraph.load function:")
    val originalGraph: Option[NetGraph] = NetGraph.load({args(6)}, {args(7)})
    val perturbedGraph: Option[NetGraph] = NetGraph.load({args(8)}, {args(9)})

    logger.info("Gathering information of the graphs")
    val netOriginalGraph: NetGraph = originalGraph.get // getiting original graph info
    val netPerturbedGraph: NetGraph = perturbedGraph.get // getiting perturbed graph info

    logger.info("Storing nodes for in a list for original graph")
    val originalGraphNodes: java.util.Set[NodeObject] = netOriginalGraph.sm.nodes()
    //val originalNodeList = originalGraphNodes.asScala.toList

    logger.info("Storing nodes for in a list for perturbed graph")
    val perturbedGraphNodes: java.util.Set[NodeObject] = netPerturbedGraph.sm.nodes()
    //val perturbedNodeList = perturbedGraphNodes.asScala.toList


    logger.info("Storing The Edges in a list for both Original Graph and the Perturbed Graph")
    val originalGraphEdges: java.util.Set[EndpointPair[NodeObject]] = netOriginalGraph.sm.edges()
    val originalEdgeList = originalGraphEdges.asScala.toList
    val perturbedGraphEdges: java.util.Set[EndpointPair[NodeObject]] = netPerturbedGraph.sm.edges()
    val perturbedEdgeList = perturbedGraphEdges.asScala.toList



    logger.info("Creating a CSV file with combination of each node of original with all nodes in perturbed")
    // Args 0 represents the file path to creating csv with combination of O x P graph nodes
    writeNodesCsvFile({args(0)}, originalGraphNodes, perturbedGraphNodes)

    logger.info("Creating a CSV file with combination of each edge of original with all edges in perturbed")
    // Args 0 represents the file path to creating csv with combination of O x P graph edges
    writeEdgesCsvFile({args(3)}, originalGraphEdges, perturbedGraphEdges)

    logger.info("Sharding the Nodes CSV files into 10's")
    shardGraphsForNode({args(1)}, {args(2)}, 10)

    logger.info("Sharding the Edges CSV files into 10's")
    shardGraphsForNode({args(4)}, {args(5)}, 10)

  }

}

/*
object file {
  def main(args: Array[String]): Unit = {


    // Creating combinations of original and perturb nodes so each original node is compared with all of perturbed nodes
    val filePath = "/Users/muzza/desktop/CS440/originalXperturbed/combinations.csv"
    writeNodesCsvFile(filePath, originalGraphNodes, perturbedGraphNodes)

    // Taking the combination nodes of original and perturbed generated in writeNodesCsvFile, we will shard it
    val input = "/Users/muzza/desktop/CS440/originalXperturbed/combinations.csv"
    val output = "/Users/muzza/desktop/CS440/shardedFiles"
    shardGraphsForNode(input, output, 10)

    //-----------------------------------------

    val fileEdgePath = "/Users/muzza/desktop/CS440/originalPerturbedEdges/combinedEdges.csv"
    writeEdgesCsvFile(fileEdgePath, originalGraphEdges, perturbedGraphEdges)


    // Calling the function to shard the edges:
    val inputToShardEdges = "/Users/muzza/desktop/CS440/originalPerturbedEdges/combinedEdges.csv"
    val outputForShardedEdges = "/Users/muzza/desktop/CS440/shardedFileEdges"
    shardGraphsForNode(inputToShardEdges, outputForShardedEdges, 10)
  }
}
*/

