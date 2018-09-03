package ch.gadp.kafka.tools

import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.slf4j.LoggerFactory
import java.util.*


private val logger = LoggerFactory.getLogger("KafkaUtils")


/**
 * Builds the consumer. We choose byte arrays to avoid any conversion
 */
fun buildConsumer(kafkaServer: String, groupId: String): Consumer<ByteArray, ByteArray> {
    val properties = Properties()
    properties["bootstrap.servers"] = kafkaServer
    properties["group.id"] = groupId
    properties["enable.auto.commit"] = "false"
    properties["max.poll.interval.ms"] = 200
    properties["max.poll.records"] = 10
    properties["key.deserializer"] = "org.apache.kafka.common.serialization.ByteArrayDeserializer"
    properties["value.deserializer"] = "org.apache.kafka.common.serialization.ByteArrayDeserializer"
    properties["auto.offset.reset"] = "earliest"

    return KafkaConsumer(properties)
}

/**
 * Builds the producer.
 */
fun buildProducer(kafkaServer: String, mock: Boolean): Producer<ByteArray, ByteArray> =
        when (mock) {
            false -> {
                val properties = Properties()
                properties["bootstrap.servers"] = kafkaServer
                properties["acks"] = "all"
                properties["retries"] = 0
                properties["batch.size"] = 16384
                properties["linger.ms"] = 1
                properties["buffer.memory"] = 33554432
                properties["key.serializer"] = "org.apache.kafka.common.serialization.ByteArraySerializer"
                properties["value.serializer"] = "org.apache.kafka.common.serialization.ByteArraySerializer"

                KafkaProducer<ByteArray, ByteArray>(properties)
            }
            true -> MockProducer<ByteArray, ByteArray>(false, ByteArraySerializer(), ByteArraySerializer())
        }


fun writeRecords(producer: Producer<ByteArray, ByteArray>, toTopic: String, records: ArrayList<ConsumerRecord<ByteArray, ByteArray>>) {
    records.forEach {
        logger.info("Copying message at offset ${it.offset()} with timestamp ${it.timestamp()}")
        producer.send(ProducerRecord<ByteArray, ByteArray>(toTopic, null, it.timestamp(), it.key(), it.value()))
    }
    producer.flush()
}
