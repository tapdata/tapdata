import io.tapdata.connector.kafka.config.KafkaConfig;
import io.tapdata.connector.kafka.config.ProducerConfiguration;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.UUID;

public class Main {
    public static void main(String[] args) {
        KafkaConfig kafkaConfig = new KafkaConfig();
        kafkaConfig.setNameSrvAddr("10.1.42.76:9094,10.1.42.77:9094,10.1.42.78:9094");
        kafkaConfig.setMqUsername("kafka");
        kafkaConfig.setMqPassword("BigData@000");
        kafkaConfig.setKafkaSaslMechanism("SHA256");
        kafkaConfig.setKafkaAcks("-1");
        kafkaConfig.setKrb5(false);
        ProducerConfiguration producerConfiguration = new ProducerConfiguration(kafkaConfig, null);
        new KafkaProducer<>(producerConfiguration.build());
    }
}
