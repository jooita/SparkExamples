import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.kafka.common.serialization.StringDeserializer

import java.util

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat
import java.util.concurrent.atomic._


object WordCount{
	
	val usage = """
	Usage:  spark-submit ~ -t topics [-b brokers] [-p port] [-n node] ...
	Where:  -t topic  Set Kafka stream topic name to consume (split: , )
		-b broker Set Kafka broker names.  defalut-> localhost:9092
		-pt Set time(sec)
	"""

	var topic: String = "topicName"
	var broker: String = "kafka:9092"
	var nMsg: Int = 0
	var ms: Int = 0
	var core: String = "1"
	val unknown = "(^-[^\\s])".r

	var np = 0
	var nb:Long = 0
	var startMs = 0L
	var endMs = 0L

	val pf: PartialFunction[List[String], List[String]] = {
		
	case "-t" :: (arg: String) :: tail =>
		topic = arg; tail
	case "-b" :: (arg: String) :: tail =>
		broker = arg; tail
	case "-nMsg" :: (arg: String) :: tail =>
		nMsg = arg.toInt; tail
	case "-ms" :: (arg: String) :: tail =>
		ms = arg.toInt; tail
	case "-core" :: (arg: String) :: tail =>
		core = arg; tail
	case unknown(bad) :: tail => die("unknown argument " + bad + "\n" + usage)
	}

	def splitData(dat: String): Array[String] = {
		val splits = dat.split(" ")
		splits.union(Array(np.toString))
	}


	def main(args: Array[String]) {
		startMs = System.currentTimeMillis

		if (args.length != 0){
			val arglist = args.toList	
			parseArgs(arglist, pf)
		}

		val sconf = new SparkConf().setAppName("Kafka WordCount").set("spark.cores.max",core)

		val sc = new SparkContext(sconf)

		// Create context with 2 second batch interval
		// val ssc = new StreamingContext(sc, Milliseconds(ms))
		val ssc = new StreamingContext(sc, Seconds(1))

		// Create direct kafka stream with brokers and topics
		//val topics = topic.map(_.toString).toSet
		val topics = topic.split(",").toSet

		val kafkaParams = Map[String, Object](
		"metadata.broker.list" -> broker,
		"bootstrap.servers" -> broker,
		"key.deserializer" -> classOf[StringDeserializer],
		"value.deserializer" -> classOf[StringDeserializer],
		"group.id" -> "use_a_separate_group_id_for_each_stream",
		//"auto.offset.reset" -> "earliest",
		"enable.auto.commit" -> (false: java.lang.Boolean)
		)

		val messages = KafkaUtils.createDirectStream[String, String](
		ssc,
		LocationStrategies.PreferConsistent,
		ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
		)

		val df = new SimpleDateFormat("MM-dd HH:mm:ss:SSS")

		//val words = messages.map(_.value).flatMap(_.split(" "))
		val words = messages.map(record => record.value()).flatMap(splitData)

		val wordCounts = words.map(x => (x, 1L))
		.reduceByKey(_ + _).print()

		// Start the computation
		ssc.start()
		ssc.awaitTermination()

	}

	def parseArgs(args: List[String], pf: PartialFunction[List[String], List[String]]): List[String] = args match {
	case Nil => Nil
	case _ => if (pf isDefinedAt args) parseArgs(pf(args), pf)
		else args.head :: parseArgs(args.tail, pf)
	}

	def die(msg: String = usage) = {
		println(msg)
		sys.exit(1)
	}

}


