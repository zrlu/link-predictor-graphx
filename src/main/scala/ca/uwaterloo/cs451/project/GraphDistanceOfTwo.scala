package ca.uwaterloo.cs451.project

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.rdd.RDD
import org.rogach.scallop._
import org.apache.hadoop.fs._

import scala.collection.immutable.{HashMap,HashSet}

object GraphDistanceOfTwo {

  class Conf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(vert, eold, output)
    val vert = opt[String](descr = "vertices file", required = true)
    val eold = opt[String](descr = "old edges file", required = true)
    val output = opt[String](descr = "output path", required = true)
    verify()
  }

  def main(argv: Array[String]) {
    val args = new Conf(argv)
      val conf = new SparkConf().setAppName("GraphDistanceOfTwo")
      val sc = new SparkContext(conf)
      FileSystem.get(sc.hadoopConfiguration).delete(new Path(args.output()), true)

      val authorsRDD = sc.textFile(args.vert())
        .map(line => line.split('\t'))
        .map {
          case Array(vertexId, authorName) => (vertexId.toLong, authorName)
        }
      
      val collabOldRDD = sc.textFile(args.eold())
        .map(line => line.split('\t'))
        .flatMap {
          case Array(source, target) => List(
            Edge(source.toLong, target.toLong, "collab"),
            Edge(target.toLong, source.toLong, "collab")
          )
        }
      collabOldRDD.cache()

      val graph0 = Graph(authorsRDD, collabOldRDD)
      graph0.cache()

      type SPMap = Map[VertexId, Int]

      def makeMap(x: (VertexId, Int)*) = Map(x: _*)
    
      def incrementMap(spmap: SPMap): SPMap = spmap.map { case (v, d) => v -> (d + 1) }
    
      def addMaps(spmap1: SPMap, spmap2: SPMap): SPMap = {
        (spmap1.keySet ++ spmap2.keySet).map {
          k => k -> math.min(spmap1.getOrElse(k, Int.MaxValue), spmap2.getOrElse(k, Int.MaxValue))
        }(collection.breakOut)
      }
      def vertexProgram(id: VertexId, attr: SPMap, msg: SPMap): SPMap = {
        addMaps(attr, msg)
      }
      
      def sendMessage(edge: EdgeTriplet[SPMap, _]): Iterator[(VertexId, SPMap)] = {
        val newAttr = incrementMap(edge.dstAttr)
        if (edge.srcAttr != addMaps(newAttr, edge.srcAttr)) Iterator((edge.srcId, newAttr))
        else Iterator.empty
      }
  
      val spGraph = graph0.mapVertices {
        (vertexId, attr) => makeMap(vertexId -> 0)
      }
      val initialMessage = makeMap()
      
      val rand = scala.util.Random

      val scores = spGraph.pregel(initialMessage, 2)(vertexProgram, sendMessage, addMaps)
        .vertices
        .flatMap {
          case (source, spMap) => {
            spMap.map {
              case (target, distance) => ((source, target), -distance, rand.nextInt)
            }
          }
        }
        .filter {
          case ((source, target), score, randomNumber) => source < target && score == -2
        }
        .sortBy(_._3)
        .map {
          case (edge, score, randomNumber) =>
          (edge, -2)
        }

      scores.saveAsTextFile(args.output())
  }
}