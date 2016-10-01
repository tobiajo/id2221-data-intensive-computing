import java.util.HashMap

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import kafka.serializer.{DefaultDecoder, StringDecoder}

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel

object KafkaStreamAvg {

    def main(args: Array[String]) {
        val kafkaConf = Map(
    	   "metadata.broker.list"              -> "localhost:9092",
    	   "zookeeper.connect"                 -> "localhost:2181",
    	   "group.id"                          -> "kafka-spark-streaming",
    	   "zookeeper.connection.timeout.ms"   -> "1000")

        // Create a local StreamingContext with two working thread and batch interval of 1 second.
        // The master requires 2 cores to prevent from a starvation scenario.
        val sparkConf = new SparkConf().setMaster("local[2]").setAppName("KafkaStreamAvg")
        val ssc = new StreamingContext(sparkConf, Seconds(1))
        ssc.checkpoint("checkpoint")

        // receiver-less approach
        val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConf, Set("avg"))

        val values = messages.map(x => x._2)                                                        // extract value
        val pairs = values.map(x => x.split(",") match {case Array(s1, s2) => (s1, s2.toDouble)})   // String => (String, Double)

        def mappingFunc(key: String, value: Option[Double], state: State[(Double, Long)]): (String, Double) = {
            val (prevSum, prevN) = state.getOption.getOrElse((0.0, 0L))
            val (sum, n) = (prevSum+value.get, prevN+1)
            state.update((sum, n))
            (key, sum/n) // average
        }

        val stateSpec = StateSpec.function(mappingFunc _)
        val stateDstream = pairs.mapWithState(stateSpec)

        stateDstream.print()
        ssc.start()
        ssc.awaitTermination()
    }
}
