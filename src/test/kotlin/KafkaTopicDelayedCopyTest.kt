import ch.gadp.kafka.tools.buildProducer
import ch.gadp.kafka.tools.calculateTotalDelay
import ch.gadp.kafka.tools.copy
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.MockConsumer
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.TimestampType
import org.junit.Test
import kotlin.test.assertEquals

class KafkaTopicDelayedCopyTest {

    @Test
    fun should_calculate_total_delay_correctly() {
        val delayMs = 10L
        val delaySec = 20L
        val delayMin = 30L
        val delayHours = 40L
        val delayDays = 50L

        assertEquals(10L + 20L * 1000L, calculateTotalDelay(delayMs = delayMs, delaySec = delaySec))
        assertEquals(10L + 30L * 1000L * 60L, calculateTotalDelay(delayMs = delayMs, delayMin = delayMin))
        assertEquals(40L * 1000L * 60L * 60L, calculateTotalDelay(delayHours = delayHours))
        assertEquals(40L * 1000L * 60L * 60L + 50L * 1000L * 60L * 60L * 24L, calculateTotalDelay(delayHours = delayHours, delayDays = delayDays))

    }


    @Test
    fun should_copy_only_records_with_timestamp() {
        val referenceTimeMs = 1000L
        val fromTopic = "from"
        val toTopic = "to"


        val consumer = MockConsumer<ByteArray, ByteArray>(OffsetResetStrategy.EARLIEST)
        consumer.updatePartitions(fromTopic, listOf(PartitionInfo(fromTopic, 0, null, emptyArray(), emptyArray())))
        consumer.updateBeginningOffsets(mapOf(TopicPartition(fromTopic, 0) to 0L))

        consumer.assign(listOf(TopicPartition(fromTopic, 0)))

        // Create 3 records before the delay
        consumer.addRecord(ConsumerRecord<ByteArray, ByteArray>(fromTopic, 0, 0L, 100L, TimestampType.CREATE_TIME, 0, 0, 0, ByteArray(0), ByteArray(0)))
        consumer.addRecord(ConsumerRecord<ByteArray, ByteArray>(fromTopic, 0, 1L, 250L, TimestampType.CREATE_TIME, 0, 0, 0, ByteArray(0), ByteArray(0)))
        consumer.addRecord(ConsumerRecord<ByteArray, ByteArray>(fromTopic, 0, 2L, 499L, TimestampType.CREATE_TIME, 0, 0, 0, ByteArray(0), ByteArray(0)))
        consumer.addRecord(ConsumerRecord<ByteArray, ByteArray>(fromTopic, 0, 3L, 500L, TimestampType.CREATE_TIME, 0, 0, 0, ByteArray(0), ByteArray(0)))
        consumer.addRecord(ConsumerRecord<ByteArray, ByteArray>(fromTopic, 0, 4L, 750L, TimestampType.CREATE_TIME, 0, 0, 0, ByteArray(0), ByteArray(0)))
        consumer.addRecord(ConsumerRecord<ByteArray, ByteArray>(fromTopic, 0, 5L, 1000L, TimestampType.CREATE_TIME, 0, 0, 0, ByteArray(0), ByteArray(0)))
        consumer.addRecord(ConsumerRecord<ByteArray, ByteArray>(fromTopic, 0, 6L, 1200L, TimestampType.CREATE_TIME, 0, 0, 0, ByteArray(0), ByteArray(0)))


        val producer_500 = buildProducer("", true) as MockProducer
        copy(consumer, fromTopic, producer_500, toTopic, referenceTimeMs, 500L, -1)
        assertEquals(3, producer_500.history().size)
        assertEquals(100L, producer_500.history()[0].timestamp())
        assertEquals(250L, producer_500.history()[1].timestamp())
        assertEquals(499L, producer_500.history()[2].timestamp())
        producer_500.history().forEach {
            assertEquals(toTopic, it.topic())
        }


    }
}

