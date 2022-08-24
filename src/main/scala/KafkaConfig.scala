package uni.mjinor.cancer

object KafkaConfig {
  val BOOTSTRAPSERVERS : String = "127.0.0.1:9092";
  val TOPIC: String = "Twitter-Kafka";
  val ACKS_CONFIG: String = "all";
  val MAX_IN_FLIGHT_CONN: String = "5";
  val COMPRESSION_TYPE: String = "snappy";
  val RETRIES_CONFIG: String = Integer.toString(Integer.MAX_VALUE);
  val LINGER_CONFIG: String = "20";
  val BATCH_SIZE: String = Integer.toString(32*1024);
}
