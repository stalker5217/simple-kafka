package simple;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SyncCommitConsumer {
	private static final String TOPIC_NAME = "simple.2";
	private static final String BOOTSTRAP_SERVERS = "http://localhost:9092";
	private static final String GROUP_ID = "group.1";

	public static void main(String[] args) {
		try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getProperties())) {
			consumer.subscribe(List.of(TOPIC_NAME));

			while (true) {
				ConsumerRecords<String, String> kafkaRecords = consumer.poll(Duration.ofSeconds(1));
				for (var kafkaRecord : kafkaRecords) {
					log.info("{}", kafkaRecord);
				}

				consumer.commitSync();
			}
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

		properties.put(
			ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
			10
		);

		return properties;
	}
}
