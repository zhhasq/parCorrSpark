package fr.inria.zenith

import org.apache.commons.cli.{BasicParser, CommandLine, HelpFormatter, Options, ParseException}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import math._
import scala.collection.mutable.ListBuffer

/*
Example usage:
$ /tmp/spark/bin/spark-submit --class fr.inria.zenith.ParCorr ParCorr-1.0-SNAPSHOT-jar-with-dependencies.jar --tsFilePath Finance_Data.csv
*/

object ParCorr {


  private def mult(a: Array[Float], b: Array[Array[Float]], i: Int) = {
    for (col <- b)
      yield
        (a zip col.slice(i, i + a.length)).map( t => t._1 * t._2 ).sum
  }

  //Random vectors Array[Array[Floats]] generator where a  = size of Sketches, b = size of Random vector
  private def ranD(a: Int, b: Int)= {
    (for (j<-0 until a) yield
      (for (i <- 0 until b) yield (scala.util.Random.nextInt(2) * 2 - 1).toFloat).toArray).toArray
  }

  private def corr(xs: (Array[Float], Float, Float), ys: (Array[Float], Float, Float)) =
    (xs._1 zip ys._1).map { case (x, y) => (y - ys._2)*(x - xs._2)}.sum / (xs._3 * ys._3 * xs._1.length)

  private def readRmFile(rmfile: String, sizeData: Int) = {
    import scala.io.Source
    val rmstr = Source.fromFile(rmfile).getLines.next
    val rm = rmstr.sliding(sizeData, sizeData).map( s => s.map( s => (if (s=='1') 1 else -1).toFloat ).toArray ).toArray
    rm
  }

  private def initCounter(xs: Array[Long]) =
    xs.map((_, 1.toLong))

  private def mergeCounter(xs: Array[(Long, Long)], ys: Array[(Long, Long)]) = {
     var rs = new ListBuffer[(Long, Long)]()
     var i = 0

     for (x <- xs) {
       while (i < ys.length && x._1 > ys(i)._1) {
         rs += ys(i)
         i += 1
       }

       if ( i >= ys.length || x._1 < ys(i)._1)
         rs += x
       else {
         rs += (( x._1, x._2 + ys(i)._2 ))
         i += 1
       }
     }

     if (i < ys.length)
       rs ++= ys.slice(i, ys.length)

     rs.toArray
   }

  private def slidingSums[A](xs: Array[A], ws: Int, bw: Int)(implicit n: Numeric[A]) =
    // TODO: optimize
    xs.sliding(ws, bw).map(_.sum)

  private def slidingStats(xs: Array[Float], ws: Int, bw: Int) =
    (slidingSums(xs, ws, bw) zip slidingSums(xs.map(pow(_,2)), ws, bw))
      .map( t=> (t._1/ws, sqrt(t._2/ws - pow(t._1/ws, 2)).toFloat)).toArray

  private def inputRDD(sc: SparkContext, cmd: CommandLine) = {
    val tsFilePath = cmd.getOptionValue("tsFilePath", "")
    val numPart = cmd.getOptionValue("numPart", "0").toInt
    val firstCol = cmd.getOptionValue("firstCol", "2").toInt

    val distFile = if ( Array("txt", "csv").contains(tsFilePath.slice(tsFilePath.length - 3, tsFilePath.length)) )
                        (if (numPart == 0) sc.textFile(tsFilePath) else sc.textFile(tsFilePath, numPart))
                            .map( _.split(',') ).map( t => (t(0).toLong, t.slice(firstCol, t.length).map(_.toFloat) ) )
                  else
                      if (numPart == 0) sc.objectFile[(Long, (Array[Float]))](tsFilePath)
                                   else sc.objectFile[(Long, (Array[Float]))](tsFilePath, numPart)

    distFile
  }

  private def windowWithStats( dataWithStats: RDD[(Long, Array[Float], Array[(Float, Float)])], ws: Int, bw: Int, i: Int) = {
    val rdd = dataWithStats.map(t => (t._1, t._2.slice(i * bw, i * bw + ws), t._3(i)))
    val rddWithStats = rdd.map(t => (t._1, (t._2, t._3._1, t._3._2)))
    rddWithStats
  }

  private def process(sketches: RDD[(Long, Array[Float])], rddWithStats: RDD[(Long, (Array[Float], Float, Float))], cmd: CommandLine): Unit = {

    val gridDimension = cmd.getOptionValue("gridDimension", "2").toInt
    val gridSize = cmd.getOptionValue("gridSize", "1").toFloat
    val candThreshold = cmd.getOptionValue("candThresh", "2").toInt
    val corrThreshold = cmd.getOptionValue("corrThresh", "0.9").toFloat
    val listPairs = cmd.hasOption("listPairs")
    val candOnly = cmd.hasOption("candOnly")
    val compareComm = cmd.hasOption("compareComm")
    val basicComm = cmd.hasOption("basicComm")

    val groups = sketches.flatMap(sc => sc._2.sliding(gridDimension, gridDimension)
                                      .zipWithIndex
                                      .map(group => (group._2 + "|" + group._1.map(v => (v / gridSize).toInt - (if (v < 0) 1 else 0)).mkString("|"), sc._1)))

    val grids = groups.combineByKey(v => Array(v), (xs: Array[Long], v) => xs :+ v, (xs: Array[Long], ys: Array[Long]) => xs ++ ys).map(_._2.sorted)

    if (compareComm)
      grids.cache()

    val cands = grids.flatMap( v => for (i <- 0 until v.length-1) yield ( v(i), initCounter(v.slice(i+1, v.length)) ) )
        .combineByKey(v => v, (xs: Array[(Long, Long)], v) => mergeCounter(xs, v), (xs: Array[(Long, Long)], ys: Array[(Long, Long)]) => mergeCounter(xs, ys))
        .map( t => (t._1, t._2.filter(_._2 >= candThreshold).map(_._1)))
        .flatMap(  t => for (id <- t._2) yield ( t._1, id ) )

    val cands_semiopt = grids.flatMap( v => for (i <- v.indices) yield ( v(i), initCounter(v.filter(_ != v(i))) ) )
      .combineByKey(v => v, (xs: Array[(Long, Long)], v) => mergeCounter(xs, v), (xs: Array[(Long, Long)], ys: Array[(Long, Long)]) => mergeCounter(xs, ys))
      .map( t => (t._1, t._2.filter(t._1 < _._1).filter(_._2 >= candThreshold).map(_._1)) )
      .flatMap(  t => for (id <- t._2) yield ( t._1, id ) )

    val cands_basic = grids.flatMap( v => for (i <- 0 until v.length-1 ; j <- i+1 until v.length) yield ( (v(i), v(j)), 1 ) )
      .reduceByKey(_ + _)
      .filter(_._2 >= candThreshold)
      .map(kv => kv._1)

    if (compareComm) {

      val t0 = System.currentTimeMillis()
      val g = grids.count()
      val t1 = System.currentTimeMillis()
      println("Grids construction: " + (t1 - t0)  + " ms, grid cells: " + g)

      val c_opt = cands.count()
      val t2 = System.currentTimeMillis()
      println("Opt comm: " + (t2 - t1)  + " ms, candidates: " + c_opt)

      val c_semiopt = cands_semiopt.count()
      val t3 = System.currentTimeMillis()
      println("Semi-opt comm: " + (t3 - t2)  + " ms, candidates: " + c_semiopt)

      val c_basic = cands_basic.count()
      val t4 = System.currentTimeMillis()
      println("Basic comm: " + (t4 - t3)  + " ms, candidates: " + c_basic)

      return
    }

    val corrs = ( if (basicComm) cands_basic else cands)
        .join(rddWithStats).map( kv => (kv._2._1, (kv._1, kv._2._2)) )
        .join(rddWithStats).map( kv => ( kv._2._1._1
                                       , kv._1
                                       , corr(kv._2._1._2, kv._2._2) ) )
        .filter( _._3 > corrThreshold )

    val t1 = System.currentTimeMillis()

    if (listPairs) {
      val result = (if (candOnly) cands else corrs).collect()
      result.foreach(println(_))
      println(result.length)
    } else {
      println("Number of pairs: " + (if (candOnly) cands else corrs).count )
    }

    val t2 = System.currentTimeMillis()

    println("Elapsed time: " + (t2 - t1)  + " ms")
  }

  private def runParCorr(sc: SparkContext, cmd: CommandLine): Unit = {

    val ws = cmd.getOptionValue("sizeWindow", "500").toInt
    val bw = cmd.getOptionValue("basicWindow", "20").toInt
    val sizeSketches = cmd.getOptionValue("sizeSketches", "60").toInt
    val startWindow = cmd.getOptionValue("startWindow", "0").toInt
    val rmfile = cmd.getOptionValue("rmfile", "")

    val dataWithStats = inputRDD(sc, cmd).map(t => (t._1, t._2, slidingStats(t._2, ws, bw)))

    dataWithStats.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val sizeData = dataWithStats.first()._2.length
    val RandMx = if ( rmfile == "" ) ranD(sizeSketches, sizeData) else readRmFile(rmfile, sizeData)
    val RandMxBroad = sc.broadcast(RandMx)

    val i = startWindow / bw
    println("Processing window #" + i)

    val rddWithStats = windowWithStats( dataWithStats, ws, bw, i )
    val rddNorm = rddWithStats.map(t => (t._1, t._2._1.map( x => (x - t._2._2) / t._2._3 )))
    var sketches = rddNorm.map(t => (t._1, mult(t._2, RandMxBroad.value, i * bw).toArray))

    sketches.persist(StorageLevel.MEMORY_AND_DISK_SER)

    process(sketches, rddWithStats, cmd)

    for (i <- (startWindow / bw + 1) until  ((sizeData-ws) / bw + 1) ) {
      println("Processing window #" + i)

      val rvSumPrev = sc.broadcast(RandMx.map( _.slice((i-1) * bw, (i-1) * bw + ws).sum ).toArray)
      val rvSumCurr = sc.broadcast(RandMx.map( _.slice(i * bw, i * bw + ws).sum ).toArray)

      val deltaWithStats = dataWithStats.map(t => (t._1, t._2.slice((i-1) * bw, i * bw),
        t._2.slice((i-1) * bw + ws, i * bw + ws),
        t._3(i-1), t._3(i)))
      // _2 = outdated basic window, _3 = incoming basic window, _4 = stats of previous window, _5 = stats of current window

      val deltaSketchWithStats = deltaWithStats.map(t => (t._1, ((mult(t._2, RandMxBroad.value, (i-1) * bw) zip mult(t._3, RandMxBroad.value, (i-1) * bw + ws)).map(p => -p._1 + p._2), t._4, t._5)))
      // _2._1 = deltaS, _2._2 = stats of previous window, _2._3 = stats of current window

      val newSketches = sketches.join(deltaSketchWithStats)
        // _1 = id, _2._1 = sketch, _2._2._1 = deltaS, _2._2._2 = stats of previous window, _2._2._3 = stats of current window
        .map{ case (id, (sketch, (deltaS, (muPrev, sigmaPrev), (muCurr, sigmaCurr)))) =>
            ( id, (sketch zip deltaS zip rvSumPrev.value zip rvSumCurr.value).map{ case (((s, ds), rp), rc) => (sigmaPrev * s + ds + muPrev * rp - muCurr * rc) / sigmaCurr } ) }

      newSketches.persist(StorageLevel.MEMORY_AND_DISK_SER)

      process(newSketches, windowWithStats( dataWithStats, ws, bw, i ), cmd)

      sketches.unpersist()
      sketches = newSketches

      rvSumCurr.unpersist()
      rvSumPrev.unpersist()
    }

  }

  def main(args: Array[String]): Unit = {
    val options = new Options()
    options.addOption("help", false, "Print help and exit ")
    options.addOption("tsFilePath", true, "Path to the Time Series input file [Mandatory]")
    options.addOption("firstCol", true, "For CSV files: the index of the column from which the time series start [Default: 2]")
    options.addOption("numPart", true, "Number of partitions for parallel data processing [Default: 0, i.e. <spark default>]")
    options.addOption("sizeWindow", true, "Size of window [Default: 500]")
    options.addOption("basicWindow", true, "Size of basic window [Default: 20]")
    options.addOption("sizeSketches", true, "Number of sketches [Default: 60]")
    options.addOption("gridDimension", true, "Number of grid dimensions [Default: 2]")
    options.addOption("gridSize", true, "Size of grid cell [Default: 1]")
    options.addOption("candThresh", true, "Candidate threshold, i.e. number of grids of collocation [Default: 2] ")
    options.addOption("corrThresh", true, "Correlation threshold [Default: 0.9] ")
    options.addOption("listPairs", false, "List all correlated pairs")
    options.addOption("candOnly", false, "List only candidates")
    options.addOption("basicComm", false, "Use basic communication strategy")
    options.addOption("compareComm", false, "Compare communication strategies")
    options.addOption("startWindow", true, "The index of window from which to start [Default: 0] ")
    options.addOption("rmfile", true, "Random matrix file [Default: <generate random matrix>] ")

    var cmd: CommandLine = null
    var printHelp = false

    try {
      val clParser = new BasicParser()
      cmd = clParser.parse(options, args)

      printHelp = cmd.hasOption("help") || cmd.getOptionValue("tsFilePath", "") == ""
    } catch {
      case x: ParseException => printHelp = true
    }

    if (printHelp) {
      val formatter = new HelpFormatter
      formatter.printHelp("ParCorr -tsFilePath <path to input file> [options]", options)
      return
    }

    val conf: SparkConf = new SparkConf().setAppName("ParCorr")
    val sc: SparkContext = new SparkContext(conf)

    runParCorr(sc, cmd)
  }
}
