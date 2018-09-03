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

class KafkaTopicCopyIntegrationTest {

    @Test
    fun should_copy_only_part() {
        val fromTopic = "copy_test_from_topic"
        val toTopic = "copy_test_to_topic"
        val groupId = "copy_test_group_id"
        val kafka = "localhost:9092"

        cleanEnvironment(kafkaServer = kafka, topics = listOf(fromTopic, toTopic), groupIds = listOf(groupId))

        addRecords(kafka, fromTopic, 6)

        processCopy(
                fromKafka = kafka,
                fromTopic = fromTopic,
                toTopic = toTopic,
                groupId = groupId,
                startPosition = 0
        )

        // Check that we have copied all records in the toTopic
        val toConsumer = buildConsumer(kafka, groupId)
        toConsumer.subscribe(listOf(toTopic))
        toConsumer.seekToBeginning(toConsumer.assignment())

        val toRecords01 = toConsumer.poll(Duration.ofMillis(1000))
        assertEquals(6, toRecords01.count())


        val fromConsumer = buildConsumer(kafka, groupId)
        fromConsumer.subscribe(listOf(fromTopic))

        // Check that we don't have any record left in the from topic for the goup id
        val fromRecords01 = fromConsumer.poll(Duration.ofMillis(1000))
        assertEquals(0, fromRecords01.count())


        // Add new records
        addRecords(kafka, fromTopic, 20)

        processCopy(
                fromKafka = kafka,
                fromTopic = fromTopic,
                toTopic = toTopic,
                groupId = groupId,
                startPosition = 0
        )

        // Check that toConsumer contains the new messages
        val toRecords02 = toConsumer.poll(Duration.ofMillis(1000))
        assertEquals(10, toRecords02.count())

        // Check that we don't have any record left in the from topic for the goup id
        val fromRecords02 = fromConsumer.poll(Duration.ofMillis(1000))
        assertEquals(0, fromRecords02.count())

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
    private fun addRecords(kafkaServer: String, topic: String, count:Int) {

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

        val random = Random()
        for (i in (1..count)) {
            val key = random.nextInt(100000)
            producer.send(ProducerRecord<String, String>(topic, "Key-$key", "Data for key $key"))
        }

        producer.flush()
        producer.close()
    }
}