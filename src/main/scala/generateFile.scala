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
import scala.collection.parallel.ParSeq
import scala.util.{Failure, Success}
import scala.jdk.CollectionConverters.*
import scala.io.Source


object GenerateFileApp {
  def main(args: Array[String]): Unit = {
    val generateFile = new GenerateFile()
    generateFile.processFile()
  }
}

class GenerateFile {

  def processFile(): Unit = {



    val logger = LoggerFactory.getLogger(this.getClass)

    // --------------------------------------------------------------------------

    logger.info("Loading in original graph ngs and perturbed graph ngs files using NetGraph.load function:")
    val originalGraph: Option[NetGraph] = NetGraph.load("NetGameSimhomework.ngs", "/Users/muzza/desktop/CS440/")
    val perturbedGraph: Option[NetGraph] = NetGraph.load("NetGameSimhomework.ngs.perturbed", "/Users/muzza/desktop/CS440/")


    logger.info("Gathering information of the graphs")
    val netOriginalGraph: NetGraph = originalGraph.get // getiting original graph info
    val netPerturbedGraph: NetGraph = perturbedGraph.get // getiting perturbed graph info

    logger.info("Storing nodes for in a list for original graph")
    val originalGraphNodes: java.util.Set[NodeObject] = netOriginalGraph.sm.nodes()
    val originalNodeList = originalGraphNodes.asScala.toList

    logger.info("Storing nodes for in a list for perturbed graph")
    val perturbedGraphNodes: java.util.Set[NodeObject] = netPerturbedGraph.sm.nodes()
    val perturbedNodeList = perturbedGraphNodes.asScala.toList


    // -------------------------------------------------------------------------

    // MAKING CSV FILES WITH ALL POSSIBLE COMBINATIONS Of NODES

    def writeNodesCsvFile(filePath: String, originalNodes: java.util.Set[NodeObject], perturbedNodes: java.util.Set[NodeObject]): Unit = {
      val writer = new BufferedWriter(new FileWriter(filePath))

      // Write the CSV header
      writer.write("originalId,originalChildren,originalProps,originalCurrentDepth,originalPropValueRange," +
        "originalMaxDepth,originalMaxBranchingFactor,originalMaxProperties,originalStoredValue," +
        "perturbedId,perturbedChildren,perturbedProps,perturbedCurrentDepth,perturbedPropValueRange," +
        "perturbedMaxDepth,perturbedMaxBranchingFactor,perturbedMaxProperties,perturbedStoredValue\n")

      // Iterate through each original node and compare it with all perturbed nodes
      originalNodes.forEach { originalNode =>
        perturbedNodes.forEach { perturbedNode =>
          //val originalNodeId = if (originalNode != null) originalNode.id.toString else ""
          val perturbedNodeId = if (perturbedNode != null) perturbedNode.id.toString else ""

          // Concatenate all values into a single string and write to the file
          val line = s"${originalNode.id},${originalNode.children},${originalNode.props},${originalNode.currentDepth},${originalNode.propValueRange}," +
            s"${originalNode.maxDepth},${originalNode.maxBranchingFactor},${originalNode.maxProperties},${originalNode.storedValue}," +
            s"${perturbedNode.id},${perturbedNode.children},${perturbedNode.props},${perturbedNode.currentDepth},${perturbedNode.propValueRange}," +
            s"${perturbedNode.maxDepth},${perturbedNode.maxBranchingFactor},${perturbedNode.maxProperties},${perturbedNode.storedValue}\n"

          writer.write(line)
        }
      }

      writer.close()
    }

    val filePath = "/Users/muzza/desktop/CS440/originalXperturbed/combinations.csv"
    writeNodesCsvFile(filePath, originalGraphNodes, perturbedGraphNodes)





    // --------------------------------------------------------------------------

    logger.info("Shards the graphs into many csv files for mapper/reducer to later process")
    def shardGraphs(inputFilePath: String, outputFolderPath: String, chunkSize: Int): Unit = {
      val source = Source.fromFile(inputFilePath)
      val lines = source.getLines().toList
      source.close()

      val header = lines.head
      val data = lines.tail

      val chunkedData = data.grouped(chunkSize).toList

      for ((chunk, index) <- chunkedData.zipWithIndex) {
        val outputFilePath = s"$outputFolderPath/shard$index.csv"
        val writer = new PrintWriter(new File(outputFilePath))
        writer.println(header)
        chunk.foreach(writer.println)
        writer.close()
      }
    }

    val input = "/Users/muzza/desktop/CS440/originalXperturbed/combinations.csv"
    val output = "/Users/muzza/desktop/CS440/shardedFiles"
    shardGraphs(input, output, 10)


    // --------------------------------------------------------------------------


    // --------------------------------------------------------------------------


    logger.info("Storing The Edges in a list for both Original Graph and the Perturbed Graph")
    val originalGraphEdges: java.util.Set[EndpointPair[NodeObject]] = netOriginalGraph.sm.edges()
    val originalEdgeList = originalGraphEdges.asScala.toList
    val perturbedGraphEdges: java.util.Set[EndpointPair[NodeObject]] = netPerturbedGraph.sm.edges()
    val perturbedEdgeList = perturbedGraphEdges.asScala.toList

    // Iterate through the list of edges and print them
        perturbedEdgeList.foreach { edge =>

          val uNode = edge.nodeU()
          val vNode = edge.nodeV()
          val source = edge.source()
          val target = edge.target()
          val adjacent = edge.adjacentNode(edge.source())

          if(source.id == 0) {
            println(s"Perturbed Edge: $edge")
            println(s"Source Node: $source")
            println(s"Edge: U Node $uNode")
            println(s"V Node $vNode")
            println(s"target Node: $target")
            println(s"Adjacent Node: $adjacent")
            println
            println
          }
        }

    originalEdgeList.foreach { edge =>

      val o_uNode = edge.nodeU()
      val o_vNode = edge.nodeV()
      val o_source = edge.source()
      val o_target = edge.target()
      val o_adjacent = edge.adjacentNode(edge.source())

      if (o_source.id == 0) {
        println(s"Original Edge: $edge")
        println(s"OSource Node: $o_source")
        println(s"OEdge: U Node $o_uNode")
        println(s"O-V Node $o_vNode")
        println(s"O-target Node: $o_target")
        println(s"O-Adjacent Node: $o_adjacent")
        println
        println
      }
    }



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

    val fileEdgePath = "/Users/muzza/desktop/CS440/originalPerturbedEdges/combinedEdges.csv"
    writeEdgesCsvFile(fileEdgePath, originalGraphEdges, perturbedGraphEdges)



    // Calling the function to shard the edges:
    val inputToShardEdges = "/Users/muzza/desktop/CS440/originalPerturbedEdges/combinedEdges.csv"
    val outputForShardedEdges = "/Users/muzza/desktop/CS440/shardedFileEdges"
    shardGraphs(inputToShardEdges, outputForShardedEdges, 10)
  }

}