package ch.gadp.kafka.tools

import org.apache.kafka.clients.consumer.*
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.docopt.Docopt
import org.slf4j.LoggerFactory
import java.util.*
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import java.time.Duration
import java.util.ArrayList


private val doc = """
    Usage:
    KafkaTopicDelayedCopy [--from-kafka FROM_KAFKA] --from FROM_TOPIC [--to-kafka TO_KAFKA] --to TO_TOPIC --group-id GROUP_ID [--delay-ms DELAY_MS] [--delay-sec DELAY_SEC] [--delay-min DELAY_MIN] [--delay-hours DELAY_HOURS] [--delay-days DELAY_DAYS] [--dry-run] [--start-position START_POSITION]

    Options:
    --from-kafka FROM_KAFKA         The Kafka source bootstrap servers list including port. [default: localhost:9092]
    --to-kafka TO_KAFKA             The Kafka destination bootstrap servers list including port. If omitted, same as specified in the argument --from-kafka
    --from FROM_TOPIC               The name of the topic to read messages from
    --to TO_TOPIC                   The name of the topic where copy the messages
    --group-id GROUP_ID             The consumer group to use for reading and writing the messages
    --delay-ms DELAY_MS             The delay in milliseconds. This value adds up to the other --delay-* arguments. [default: 0]
    --delay-sec DELAY_SEC           The delay in seconds. This value adds up to the other --delay-* arguments. [default: 0]
    --delay-min DELAY_MIN           The delay in minutes. This value adds up to the other --delay-* arguments. [default: 0]
    --delay-hours DELAY_HOURS       The delay in hours. This value adds up to the other --delay-* arguments. [default: 0]
    --delay-days DELAY_DAYS         The delay in days. This value adds up to the other --delay-* arguments. [default: 0]
    --dry-run                       Does not copy. Only log the record offset copied
    --start-position START_POSITION Specifies if the offset from which start the copy process. A value of -1 means current position. [default: -1]

"""

private val logger = LoggerFactory.getLogger("KafkaTopicDelayedCopy")


fun main(args: Array<String>) {

    Thread.setDefaultUncaughtExceptionHandler { thread, throwable ->
        logger.error("Uncaught exception in $thread:", throwable)
    }

    val opts = Docopt(doc).parse(args.toList())

    val fromKafka = opts["--from-kafka"].toString()
    val fromTopic = opts["--from"].toString()
    val toKafka = (opts["--to-kafka"] ?: fromKafka).toString()
    val toTopic = opts["--to"] as String
    val groupId = opts["--group-id"] as String
    val delayMs = opts["--delay-ms"].toString().toLong()
    val delaySec = opts["--delay-sec"].toString().toLong()
    val delayMin = opts["--delay-min"].toString().toLong()
    val delayHours = opts["--delay-hours"].toString().toLong()
    val delayDays = opts["--delay-days"].toString().toLong()
    val dryRun = opts.containsKey("--dry-run")
    val startPosition = opts["--start-position"].toString().toLong()

    process(fromKafka, fromTopic, toKafka, toTopic, groupId, delayMs, delaySec, delayMin, delayHours, delayDays, dryRun, startPosition)
}


fun process(fromKafka: String,
            fromTopic: String,
            toKafka: String = fromKafka,
            toTopic: String,
            groupId: String,
            delayMs: Long = 0L,
            delaySec: Long = 0L,
            delayMin: Long = 0L,
            delayHours: Long = 0L,
            delayDays: Long = 0L,
            dryRun: Boolean = false,
            startPosition: Long = -1L,
            referenceTimeMs: Long = System.currentTimeMillis()) {

    val totalDelayMs = calculateTotalDelay(delayMs, delaySec, delayMin, delayHours, delayDays)

    val consumer = buildConsumer(kafkaServer = fromKafka, groupId = groupId)
    consumer.subscribe(listOf(fromTopic))

    val producer = buildProducer(kafkaServer = toKafka, mock = dryRun)

    copy(
            consumer = consumer,
            fromTopic = fromTopic,
            producer = producer,
            toTopic = toTopic,
            referenceTimeMs = referenceTimeMs,
            delayMs = totalDelayMs,
            startPosition = startPosition
    )

    producer.close()
    consumer.close()
}


/**
 * Copy the messages until the first message which timestamp is after the start time minus the delay
 */

fun copy(
        consumer: Consumer<ByteArray, ByteArray>,
        fromTopic: String,
        producer: Producer<ByteArray, ByteArray>,
        toTopic: String,
        referenceTimeMs: Long,
        delayMs: Long,
        startPosition: Long
) {

    val partitions = consumer.partitionsFor(fromTopic)

    if (startPosition > -1) {
        // Dummy poll call to allow seek
        consumer.poll(Duration.ofMillis(1))
        for (topicPartition in consumer.assignment()) {
            consumer.commitSync(mapOf(topicPartition to OffsetAndMetadata(startPosition)))
        }
    }

    val timestampLimit = referenceTimeMs - delayMs

    val buffer = ArrayList<ConsumerRecord<ByteArray, ByteArray>>()
    val maxEmptyBatches = 10

    var emptyBatchCount = 0
    var lastOffset = -1L
    logger.info("Starting processing messages with timestamps up to $timestampLimit")

    while (emptyBatchCount < maxEmptyBatches) {
        val records = consumer.poll(Duration.ofMillis(100))
        for (record in records) {
            // Only add record if their timestamp is before the current time - the delay
            if (record.timestamp() < timestampLimit) {
                buffer.add(record)
            }
        }
        // Write the records and commit the offset when the buffer is full
        if (buffer.isNotEmpty()) {
            writeRecords(producer = producer, toTopic = toTopic, records = buffer)
            lastOffset = buffer.last().offset()
            buffer.clear()
        } else if (buffer.isEmpty() && records.isEmpty) {
            emptyBatchCount++
        }
    }

    // Only if we have copied some records we update the consumer offset
    if (lastOffset != -1L) {
        for (topicPartition in consumer.assignment()) {
            consumer.commitSync(mapOf(topicPartition to OffsetAndMetadata(lastOffset + 1)))
        }
    }

    logger.info("Done processing messages with timestamps up to $timestampLimit")
}

fun calculateTotalDelay(
        delayMs: Long = 0L,
        delaySec: Long = 0L,
        delayMin: Long = 0L,
        delayHours: Long = 0L,
        delayDays: Long = 0L
) = delayMs +
        delaySec * 1000L +
        delayMin * 1000L * 60L +
        delayHours * 1000L * 60L * 60L +
        delayDays * 1000L * 60L * 60L * 24L


private fun writeRecords(producer: Producer<ByteArray, ByteArray>, toTopic: String, records: ArrayList<ConsumerRecord<ByteArray, ByteArray>>) {
    records.forEach {
        logger.info("Copying message at offset ${it.offset()} with timestamp ${it.timestamp()}")
        producer.send(ProducerRecord<ByteArray, ByteArray>(toTopic, null, it.timestamp(), it.key(), it.value()))
    }
    producer.flush()
}


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
                properties["buffer.memory"] = 33554432;
                properties["key.serializer"] = "org.apache.kafka.common.serialization.ByteArraySerializer"
                properties["value.serializer"] = "org.apache.kafka.common.serialization.ByteArraySerializer"

                KafkaProducer<ByteArray, ByteArray>(properties)
            }
            true -> MockProducer<ByteArray, ByteArray>(false, ByteArraySerializer(), ByteArraySerializer())
        }
