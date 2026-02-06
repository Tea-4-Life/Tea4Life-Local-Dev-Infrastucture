package me.huynhducphu;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.EventListenerProviderFactory;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;
import org.keycloak.provider.ProviderConfigProperty;
import org.keycloak.provider.ProviderConfigurationBuilder;

import java.util.List;
import java.util.Properties;


/**
 * FACTORY
 */
public class KafkaEventListenerProviderFactory implements EventListenerProviderFactory {

    private KafkaProducer<String, String> producer;
    private String topic;

    private final String BOOTSTRAP_SERVERS_KEY = "bootstrapServers";
    private final String TOPIC_KEY = "topic";

    private final String BOOTSTRAP_SERVERS_DEFAULT_VALUE = "kafka:19092";
    private final String TOPIC_KEY_DEFAULT_VALUE = "user-registration";

    /**
     * ====================================
     * CẤU HÌNH HIỂN THỊ
     * Tạo các ô nhập liệu trên Admin Panel
     * cho trường Kafka và Topic
     * ====================================
     */
    @Override
    public List<ProviderConfigProperty> getConfigMetadata() {
        return ProviderConfigurationBuilder
                .create()

                // Boostrap Server
                .property()
                .name(BOOTSTRAP_SERVERS_KEY)
                .label("Kafka Bootstrap Servers")
                .helpText("Địa chỉ Kafka")
                .type(ProviderConfigProperty.STRING_TYPE)
                .defaultValue(BOOTSTRAP_SERVERS_DEFAULT_VALUE)
                .add()

                // Topic
                .property()
                .name(TOPIC_KEY)
                .label("Kafka Topic")
                .helpText("Tên topic")
                .type(ProviderConfigProperty.STRING_TYPE)
                .defaultValue(TOPIC_KEY_DEFAULT_VALUE)
                .add()

                .build();
    }

    /**
     * ====================================
     * THIẾT LẬP KẾT NỐI TỚI KAFKA.
     * Đọc cấu hình từ Admin Panel, nếu trống thì
     * dùng mặcđịnh. Khởi tạo producer một lần
     * duy nhất.
     * ====================================
     */
    @Override
    public void init(org.keycloak.Config.Scope scope) {

        String bootstrapServers = scope.get(BOOTSTRAP_SERVERS_KEY, BOOTSTRAP_SERVERS_DEFAULT_VALUE);
        this.topic = scope.get(TOPIC_KEY, TOPIC_KEY_DEFAULT_VALUE);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        this.producer = new KafkaProducer<>(props);
    }

    /**
     * ====================================
     * HÀM KHỞI TẠO KHI CÓ PHIÊN LÀM VIỆC.
     * Tạo Provider cho mỗi session dùng chung
     * producer đã khởi tạo ở hàm trên.
     * ====================================
     */
    @Override
    public EventListenerProvider create(KeycloakSession keycloakSession) {
        return new KafkaEventListenerProvider(producer, topic);
    }

    @Override
    public void close() {
        if (producer != null) producer.close();
    }

    @Override
    public String getId() {
        return "tea4life-kafka-listener";
    }

    @Override
    public void postInit(KeycloakSessionFactory keycloakSessionFactory) {

    }


}
