package ch.gadp.kafka.tools

import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.Producer
import org.docopt.Docopt
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.ArrayList

val kafkaTopicCopyDoc = """
    Usage:
    KafkaTopicCopy [--from-kafka FROM_KAFKA] --from FROM_TOPIC [--to-kafka TO_KAFKA] --to TO_TOPIC --group-id GROUP_ID [--dry-run] [--start-position START_POSITION]

    Options:
    --from-kafka FROM_KAFKA         The Kafka source bootstrap servers list including port. [default: localhost:9092]
    --to-kafka TO_KAFKA             The Kafka destination bootstrap servers list including port. If omitted, same as from-kafka
    --from FROM_TOPIC               The topic to read messages from
    --to TO_TOPIC                   The topic to write messages to
    --group-id GROUP_ID             The consumer group to remember the consumed offsets.
    --start-position START_POSITION An optional offset from which to start the copy. If omitted, start from where we left off
    --dry-run                       Does not copy. Only log the record offset copied
"""


private val logger = LoggerFactory.getLogger("KafkaTopicCopy")


fun main(args: Array<String>) {
    Thread.setDefaultUncaughtExceptionHandler { thread, throwable ->
        logger.error("Uncaught exception in $thread:", throwable)
    }

    val opts = Docopt(kafkaTopicCopyDoc).parse(args.toList())

    val fromKafka = opts["--from-kafka"].toString()
    val fromTopic = opts["--from"].toString()
    val toKafka = (opts["--to-kafka"] ?: fromKafka).toString()
    val toTopic = opts["--to"] as String
    val groupId = opts["--group-id"] as String
    val dryRun = opts.containsKey("--dry-run")
    val startPosition = (opts["--start-position"] ?: -1).toString().toLong()

    processCopy(fromKafka, fromTopic, toKafka, toTopic, groupId, dryRun, startPosition)
}

fun processCopy(fromKafka: String,
                       fromTopic: String,
                       toKafka: String = fromKafka,
                       toTopic: String,
                       groupId: String,
                       dryRun: Boolean = false,
                       startPosition: Long = -1L) {

    val consumer = buildConsumer(kafkaServer = fromKafka, groupId = groupId)
    consumer.subscribe(listOf(fromTopic))

    val producer = buildProducer(kafkaServer = toKafka, mock = dryRun)

    copy(
            consumer = consumer,
            producer = producer,
            toTopic = toTopic,
            startPosition = startPosition
    )

    producer.close()
    consumer.close()
}


fun copy(
        consumer: Consumer<ByteArray, ByteArray>,
        producer: Producer<ByteArray, ByteArray>,
        toTopic: String,
        startPosition: Long
) {

    if (startPosition > -1) {
        // Dummy poll call to allow seek
        consumer.poll(Duration.ofMillis(1))
        for (topicPartition in consumer.assignment()) {
            consumer.commitSync(mapOf(topicPartition to OffsetAndMetadata(startPosition)))
        }
    }

    val buffer = ArrayList<ConsumerRecord<ByteArray, ByteArray>>()
    val maxEmptyBatches = 10
    var emptyBatchCount = 0
    logger.info("Starting processing messages")

    while (emptyBatchCount < maxEmptyBatches) {
        val records = consumer.poll(Duration.ofMillis(100))
        for (record in records) {
            buffer.add(record)
        }
        // Write the records and commit the offset when the buffer is full
        if (buffer.isNotEmpty()) {
            writeRecords(producer = producer, toTopic = toTopic, records = buffer)
            consumer.commitSync()
            buffer.clear()
            emptyBatchCount = 0
        } else if (buffer.isEmpty() && records.isEmpty) {
            emptyBatchCount++
        }
    }


    logger.info("Done processing messages")
}
