package sparkstreaming

import java.util.HashMap
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._

object KafkaSpark {
  def main(args: Array[String]) {
    
    // connect to Cassandra and make a keyspace and table
    val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
    val session = cluster.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS avg_space WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
    session.execute("CREATE TABLE IF NOT EXISTS avg_space.avg (word text PRIMARY KEY, count float);")

    // Make a connection to Kafka and read (key, value) pairs from it

    // Need minimum of 2 threads, one for reading input and one for processing
    val sparkConf = new SparkConf().setAppName("KafkaSparkWordCount").setMaster("local[2]")
    val streamingContext = new StreamingContext(sparkConf, Seconds(1))
    streamingContext.checkpoint(".checkpoints/")

    val kafkaConf = Map(
      "metadata.broker.list" -> "localhost:9092",
      "zookeeper.connect" -> "localhost:2181",
      "group.id" -> "kafka-spark-streaming",
      "zookeeper.connection.timeout.ms" -> "1000")
    
    
    val topic = Set("avg")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](streamingContext, kafkaConf, topic)
    

    // measure the average value for each key in a stateful manner
    def mappingFunc(key: String, value: Option[Double], state: State[Double]): (String, Double) = {
      if (state.exists) {
        val oldState = state.get()
        val newState = (oldState + value.get) / 2
        state.update(newState)
        return (key, newState)
      } else {
        val newState = value.get
        state.update(newState)
        return (key, newState)
      }
    }

    // Format key-value pair since the original format is (null, "a,0")
    val values = messages.map(x => x._2)
    val pairs = values.map(_.split(",") ).map(x => (x(0), x(1).toDouble))
    val stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc _))

    // store the result in Cassandra
    stateDstream.saveToCassandra("avg_space", "avg", SomeColumns("word", "count"))
    stateDstream.print()

    streamingContext.start()
    streamingContext.awaitTermination()
    session.close()
  }
}
