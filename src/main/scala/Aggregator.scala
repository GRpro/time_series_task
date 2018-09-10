import java.io.PrintWriter
import java.nio.file.{Files, Path, Paths}
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.apache.commons.cli.{CommandLine, DefaultParser, HelpFormatter, Options, ParseException, Option => CliOption}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.io.{Codec, Source}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

case class Record(date: LocalDate, count: Int)

object Aggregator {

  val DATE_FORMAT = "yyyy-MM-dd"

  val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern(DATE_FORMAT)

  def dateToString(d: LocalDate): String = d.format(formatter)

  def stringToDate(s: String): LocalDate = LocalDate.parse(s, formatter)

  def deserialize(record: String): Record = {
    val splitted = record.split(":")
    val date = stringToDate(splitted(0))
    val count = splitted(1).toInt
    Record(date, count)
  }

  def serialize(record: Record): String = {
    dateToString(record.date) + ":" + record.count
  }

  // records are non-empty
  def merge(records: Iterable[Record]): Record =
    records.tail.foldLeft(records.head)((r1, r2) => r1.copy(count = r1.count + r2.count))

  // it is important to ignore invalid records properly if input files are large enough
  @tailrec
  def nextRecord(it: Iterator[String]): Option[Record] = {
    if (it.hasNext) {
      Try {
        deserialize(it.next())
      } match {
        case Success(r) =>
          Some(r)
        case Failure(NonFatal(e)) =>
          // failed to parse record, skip
          println(e.getMessage)
          nextRecord(it)
        case Failure(e) => throw e
      }
    } else None
  }

  def processRecords(storeFunc: String => Unit)(sourceIterators: Array[Iterator[String]]): Unit = {

    /*
    Take one record from previously fetched records or every non-empty iterator,
    find records with minimal timestamp, merge them into single record and write to result file
     */
    @tailrec
    def readRecordSeries(storeFunc: String => Unit,
                         sourceIterators: Array[Iterator[String]],
                         previousRecords: Array[Option[Record]]): Unit = {

      val records = sourceIterators.zipWithIndex
        .map { case (it, idx) =>
          if (previousRecords(idx).isDefined) idx -> previousRecords(idx)
          else idx -> nextRecord(it)
        }
        .withFilter { case (_, op) => op.isDefined }
        .map { case (idx, op) => idx -> op.get }

      if (records.nonEmpty) {
        val sorted = records.sortBy { case (_, rec) => rec.date.toEpochDay }
        val date = sorted.head._2.date
        val (toMerge, rest) = sorted.span { case (_, rec) => rec.date == date }

        val toWrite = merge(toMerge.map(_._2))
        val converted = serialize(toWrite)
        storeFunc(converted)

        val previousRecords = Array.fill[Option[Record]](sourceIterators.length)(None)
        rest.foreach { case (idx, rec) => previousRecords(idx) = Some(rec) }

        readRecordSeries(storeFunc, sourceIterators, previousRecords)
      }
    }

    val initialPreviousRecords: Array[Option[Record]] =
      Array.fill(sourceIterators.length)(None)

    readRecordSeries(storeFunc, sourceIterators, initialPreviousRecords)

  }

  def main(args: Array[String]): Unit = {

    val options = new Options()
    options.addOption(CliOption.builder("d").longOpt("src-dir").hasArg(true)
      .required(false)
      .desc("directory with source files, if not defined option 'f' must exist").build())

    options.addOption(CliOption.builder("f").longOpt("src-files").hasArg()
      .hasArg(true)
      .required(false)
      .desc("individual comma separated source files, if not defined option 'd' must exist").build())

    options.addOption(CliOption.builder("o").longOpt("output-file")
      .hasArg(true)
      .required(true)
      .desc("output file").build())

    options.addOption(CliOption.builder("h").longOpt("help")
      .hasArg(false)
      .required(false)
      .desc("Prints usage").build())


    def printUsage() =
      new HelpFormatter().printHelp("Aggregator", "Time series aggregator", options, "", true)

    val cmdLine: CommandLine = try {
      new DefaultParser().parse(options, args)
    } catch {
      case ex: ParseException =>
        ex.printStackTrace()
        printUsage()
        sys.exit(1)
    }

    if (cmdLine.hasOption('h')) {
      printUsage()
      sys.exit(0)
    }

    val inputFiles: Array[Path] =
      if (cmdLine.hasOption("f")) {
        cmdLine.getOptionValue("f").split(",").map(Paths.get(_).toAbsolutePath)
      } else if (cmdLine.hasOption("d")) {
        Files.list(Paths.get(cmdLine.getOptionValue("d"))).iterator().asScala.toArray
      } else {
        sys.exit(1)
      }

    val outputFile = Paths.get(cmdLine.getOptionValue("o")).toAbsolutePath
    if (Files.exists(outputFile)) Files.delete(outputFile)
    Files.createDirectories(outputFile.getParent)

    // UTF-8 is valid ASCII
    val sourceIterators = inputFiles.map(file => Source.fromFile(file.toFile)(Codec.UTF8).getLines())

    def writeFunc(pw: PrintWriter, record: String): Unit =
      pw.write(record + "\n")

    var pw: PrintWriter = null
    try {
      pw = new PrintWriter(outputFile.toFile)

      processRecords(writeFunc(pw, _))(sourceIterators)
    } finally {
      if (pw != null) pw.close()
    }
  }
}
