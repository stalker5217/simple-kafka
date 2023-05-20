package simple;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SimpleProducer {
	private static final String TOPIC_NAME = "simple.2";
	private static final String BOOTSTRAP_SERVERS = "http://localhost:9092";

	private static final AtomicInteger sequence = new AtomicInteger();

	public static void main(String[] args) {
		try (KafkaProducer<String, String> producer = new KafkaProducer<>(getProperties())) {

			ProducerRecord<String, String> kafkaRecord = new ProducerRecord<>(
				TOPIC_NAME,
				String.valueOf(sequence.incrementAndGet()),
				"Hello world!"
			);

			for (int i = 0 ; i < 100 ; i++) {
				producer.send(kafkaRecord, ((metadata, exception) -> {
					if (exception != null) {
						log.error("{}", exception.getMessage(), exception);
					} else {
						log.info("topic: {}, partition: {}, offset: {}",
							metadata.topic(),
							metadata.partition(),
							metadata.offset()
						);
					}
				}));
			}

			log.info("{}", kafkaRecord);

			producer.flush();
		}
	}

	private static Properties getProperties() {
		Properties properties = new Properties();

		properties.put(
			ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
			BOOTSTRAP_SERVERS
		);

		properties.put(
			ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
			StringSerializer.class.getName()
		);

		properties.put(
			ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
			StringSerializer.class.getName()
		);

		return properties;
	}
}
