package ch.gadp.kafka.tools

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.Test
import java.time.Duration
import java.util.*
import kotlin.test.assertEquals

class KafkaTopicDelayedCopyIntegrationTest {

    @Test
    fun should_copy_only_part() {
        val referenceTimeMs = 1000L
        val delayMs = 500L
        val fromTopic = "delayed_copy_test_from_topic"
        val toTopic = "delayed_copy_test_to_topic"
        val groupId = "delayed_copy_test_group_id"
        val kafka = "localhost:9092"

        cleanEnvironment(kafkaServer = kafka, topics = listOf(fromTopic, toTopic), groupIds = listOf(groupId))

        addRecords(kafka, fromTopic)

        processDelayedCopy(
                fromKafka = kafka,
                fromTopic = fromTopic,
                toTopic = toTopic,
                referenceTimeMs = referenceTimeMs,
                delayMs = delayMs,
                groupId = groupId,
                startPosition = 0
        )

        // Check that we have copied only 3 records in the toTopic
        val toConsumer = buildConsumer(kafka, groupId)
        toConsumer.subscribe(listOf(toTopic))
        toConsumer.seekToBeginning(toConsumer.assignment())

        val toRecords = toConsumer.poll(Duration.ofMillis(1000))
        assertEquals(2, toRecords.count())


        // Check that we have 4 remaining records in the fromTopic and that any consumer with same groupId starts at the
        // correct position
        val fromConsumer = buildConsumer(kafka, groupId)
        fromConsumer.subscribe(listOf(fromTopic))

        val fromRecords = fromConsumer.poll(Duration.ofMillis(1000))
        assertEquals(4, fromRecords.count())

    }

    /**
     * Cleans up the Kafka environment, deleting our topics and any committed offset for the consumer group
     */
    private fun cleanEnvironment(kafkaServer:String, topics:List<String>, groupIds:List<String>) {

        val properties = Properties()
        properties[AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaServer

        val adminClient = AdminClient.create(properties)

        // Cleaning the topics and group
        adminClient.deleteTopics(topics)
        adminClient.deleteConsumerGroups(groupIds)
        adminClient.close()
    }

    /**
     * Adds some records
     */
    private fun addRecords(kafkaServer: String, topic: String) {

        val properties = Properties()
        properties["bootstrap.servers"] = kafkaServer
        properties["acks"] = "all"
        properties["retries"] = 1
        properties["batch.size"] = 16384
        properties["linger.ms"] = 1
        properties["buffer.memory"] = 33554432
        properties["key.serializer"] = StringSerializer::class.java.canonicalName
        properties["value.serializer"] = StringSerializer::class.java.canonicalName

        val producer = KafkaProducer<String, String>(properties)

        // Create 3 records before the delay
        producer.send(ProducerRecord<String, String>(topic, 0, 250L, "Key-1", "Data for key 1"))
        producer.send(ProducerRecord<String, String>(topic, 0, 499L, "Key-2", "Data for key 2"))
        producer.send(ProducerRecord<String, String>(topic, 0, 500L, "Key-3", "Data for key 3"))
        producer.send(ProducerRecord<String, String>(topic, 0, 750L, "Key-4", "Data for key 4"))
        producer.send(ProducerRecord<String, String>(topic, 0, 1000L, "Key-5", "Data for key 5"))
        producer.send(ProducerRecord<String, String>(topic, 0, 1200L, "Key-6", "Data for key 6"))

        producer.flush()
        producer.close()
    }
}