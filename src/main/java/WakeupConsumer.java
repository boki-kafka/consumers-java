import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

public class WakeupConsumer {

    private static final Logger logger = LoggerFactory.getLogger(WakeupConsumer.class.getName());

    public static void main(String[] args) {
        String topicName = "simple-topic";

        Properties props = initConsumerProps(
            StringDeserializer.class,
            StringDeserializer.class
        );

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(topicName));

        Thread mainThread = Thread.currentThread();

        // main thread 종료시 별도의 thread로 kafka consumer wakeup() call
        Runtime.getRuntime().addShutdownHook(new Thread(
                () -> {
                    logger.info("main program starts to exit by calling wakeup");
                    consumer.wakeup();

                    // 메인스레드가 죽을때까지 대기하기
                    try {
                        mainThread.join();
                    } catch (InterruptedException e) {
                        logger.error(e.getMessage());
                    }
                }

            )
        );

        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords =
                    consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : consumerRecords) {
                    logger.info("record key: {}, value: {}, partition: {}",
                        record.key(), record.value(), record.partition()
                    );
                }
            }
        } catch (WakeupException e) {
            logger.error("wakeup exception has been called");
        } finally {
            consumer.close();
        }
    }

    private static <K, V> Properties initConsumerProps(
        Class<? extends Deserializer<K>> keyDeSerClass,
        Class<? extends Deserializer<V>> valueDeSerClass
    ) {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, "10.211.55.53:9092");
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, keyDeSerClass.getName());
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, valueDeSerClass.getName());
        props.put(GROUP_ID_CONFIG, "group_01");

        return props;
    }

}