# Kafka Simple Copy

This tool copies the last messages of a topic to another topic and exits gracefully when no more messages are available

## Usage

    Usage:
    KafkaTopicDelayedCopy [--from-kafka FROM_KAFKA] 
                          --from FROM_TOPIC 
                          [--to-kafka TO_KAFKA] 
                          --to TO_TOPIC 
                          --group-id GROUP_ID 
                          [--dry-run] 
                          [--start-position START_POSITION]

    Options:
    --from-kafka FROM_KAFKA         The Kafka source bootstrap servers list including port. [default: localhost:9092]
    --to-kafka TO_KAFKA             The Kafka destination bootstrap servers list including port. If omitted, same as specified in the argument --from-kafka
    --from FROM_TOPIC               The name of the topic to read messages from
    --to TO_TOPIC                   The name of the topic where copy the messages
    --group-id GROUP_ID             The consumer group to use for reading and writing the messages
    --dry-run                       Does not copy. Only log the record offset copied
    --start-position START_POSITION Specifies if the offset from which start the copy processDelayedCopy. A value of -1 means current position. [default: -1]



# Kafka Delayed Copy

This simple tool copies from one topic to another Kafka records having a timestamp before 
a given delay against a reference time (current system time).

## Use case

Sometimes, you need to process records stored in Kafka only after a certain amount of time after they were produced. 
Kafka Streams doesn't let you interrupt the processing and wait until a delta is reached. 

Running this tool let you easily schedule a copy operation every defined period and and let it copy messages that 
were created before a certain delay to another topic which can then be processed in realtime.

Example:
Let's pretend we want to process incoming real-time records only after 12 hours using a Kafka Stream process

1. Records are coming in real-time in the topic "Input"
2. Every 12 hours, we execute the scheduled copy with the parameter ```--delay-hours 12 --to DelayedInput``` 
3. The process will copy only the records which were created 12 hours before the current execution in the topic DelayedInput
4. The Kafka Stream process will read incoming messages from the topic DelayedInput in real-time


## Usage

    KafkaTopicDelayedCopy [--from-kafka FROM_KAFKA] 
                          --from FROM_TOPIC 
                          [--to-kafka TO_KAFKA] 
                          --to TO_TOPIC 
                          --group-id GROUP_ID 
                          [--delay-ms DELAY_MS] 
                          [--delay-sec DELAY_SEC] 
                          [--delay-min DELAY_MIN] 
                          [--delay-hours DELAY_HOURS] 
                          [--delay-days DELAY_DAYS] 
                          [--dry-run] 
                          [--start-position START_POSITION]

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

