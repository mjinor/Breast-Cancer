package uni.mjinor.cancer

import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.core.{Client, Constants, Hosts, HttpHosts}
import com.twitter.hbc.httpclient.auth.{Authentication, OAuth1}
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.{Logger, LoggerFactory}

import java.util.Properties
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue, TimeUnit}
import scala.jdk.CollectionConverters._

class TwitterProducer {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private var client: Client = _
  private var producer: KafkaProducer[String, String] = _
  private val msgQueue: BlockingQueue[String] = new LinkedBlockingQueue(30)
  private val trackTerms: List[String] = List("cancer")
  def main(args: Array[String]): Unit = {

  }
  def createTwitterClient(msgQueue: BlockingQueue[String]): Client = {
    /** Setting up a connection   */
    val hosebirdHosts: Hosts = new HttpHosts(Constants.STREAM_HOST)
    val hbEndpoint: StatusesFilterEndpoint = new StatusesFilterEndpoint()
    // Term that I want to search on Twitter
    hbEndpoint.trackTerms(trackTerms.asJava)
    val hosebirdAuth: Authentication = new OAuth1(TwitterConfig.CONSUMER_KEYS,TwitterConfig.CONSUMER_SECRETS,TwitterConfig.TOKEN,TwitterConfig.SECRET)
    /** Creating a client   */
    val builder: ClientBuilder = new ClientBuilder()
      .name("Hosebird-Client")
      .hosts(hosebirdHosts)
      .authentication(hosebirdAuth)
      .endpoint(hbEndpoint)
      .processor(new StringDelimitedProcessor(msgQueue))
    builder.build()
  }
  private def createKafkaProducer(): KafkaProducer[String,String] = {
    // Create producer properties
    val prop: Properties = new Properties()
    prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,KafkaConfig.BOOTSTRAPSERVERS)
    prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,new StringSerializer().getClass.getName)
    prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, new StringSerializer().getClass.getName)

    // create safe Producer
    prop.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    prop.setProperty(ProducerConfig.ACKS_CONFIG, KafkaConfig.ACKS_CONFIG);
    prop.setProperty(ProducerConfig.RETRIES_CONFIG, KafkaConfig.RETRIES_CONFIG);
    prop.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, KafkaConfig.MAX_IN_FLIGHT_CONN);

    // Additional settings for high throughput producer
    prop.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, KafkaConfig.COMPRESSION_TYPE);
    prop.setProperty(ProducerConfig.LINGER_MS_CONFIG, KafkaConfig.LINGER_CONFIG);
    prop.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, KafkaConfig.BATCH_SIZE);
    new KafkaProducer[String,String](prop)
  }
  def  run(): Unit = {
    logger.info("Setting up")
    // 1. Call the Twitter Client
    client = createTwitterClient(msgQueue)
    client.connect()
    // 2. Create Kafka Producer
    producer = createKafkaProducer()
    // Shutdown Hook
    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      logger.info("Application is not stopping!")
      client.stop()
      logger.info("Closing Producer")
      producer.close()
      logger.info("Finished closing")
    }))
    // 3. Send Tweets to Kafka
    while (!client.isDone) {
      var msg: String = null
      try {
        msg = msgQueue.poll(5, TimeUnit.SECONDS)
      } catch {
        case e: InterruptedException =>
          e.printStackTrace()
          client.stop()
      }
      if (msg != null) {
        logger.info(msg)
        producer.send(new ProducerRecord[String, String](KafkaConfig.TOPIC, null, msg), new Callback() {
          override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
            if (exception != null) {
              logger.error("Some error OR something bad happened", exception)
            }
          }
        })
      }
    }
    logger.info("\n Application End")
  }
}
