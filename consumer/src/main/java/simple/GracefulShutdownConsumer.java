package simple;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GracefulShutdownConsumer {
	private static final String TOPIC_NAME = "simple.2";
	private static final String BOOTSTRAP_SERVERS = "http://localhost:9092";
	private static final String GROUP_ID = "group.1";
	private static final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
	private static final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getProperties());

	public static void main(String[] args) throws Exception {
		Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));

		try {
			consumer.subscribe(List.of(TOPIC_NAME));

			while (true) {
				ConsumerRecords<String, String> kafkaRecords = consumer.poll(Duration.ofSeconds(1));
				for (var kafkaRecord : kafkaRecords) {
					log.info("{}", kafkaRecord);

					offsets.put(
						new TopicPartition(kafkaRecord.topic(), kafkaRecord.partition()),
						new OffsetAndMetadata(kafkaRecord.offset() + 1, null)
					);

					consumer.commitSync(offsets);
				}
			}
		} catch (WakeupException e) {
			log.warn("wakeup exception.");
		} finally {
			consumer.close();
		}
	}

	private static Properties getProperties() {
		Properties properties = new Properties();

		properties.put(
			ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
			BOOTSTRAP_SERVERS
		);

		properties.put(
			ConsumerConfig.GROUP_ID_CONFIG,
			GROUP_ID
		);

		properties.put(
			ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
			StringDeserializer.class.getName()
		);

		properties.put(
			ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
			StringDeserializer.class.getName()
		);

		properties.put(
			ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
			false
		);

		return properties;
	}
}
