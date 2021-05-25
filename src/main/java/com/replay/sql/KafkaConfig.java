package com.replay.sql;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig implements InitializingBean {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConfig.class);
    public static final String CONSUMER = "consumer";
    public static final String PRODUCER = "producer";
    public static final String TOPIC_PREFIX = "test-sql-replay_";
    @Value("${sql.replay.kafka.brokers}")
    private String brokers;
    @Value("${sql.replay.client.type:}")
    private String clientType;
    @Value("${sql.replay.app.id:}")
    private String appId;

    private String topic;

    private KafkaProducer<String, String> kafkaProducer;

    @Bean
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>(8);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test_sql_replay");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        return props;
    }

//    @Bean
    public Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>(8);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }

//    @Bean
//    @Conditional(SqlReplayProducerEnable.class)
//    public KafkaProducer<String, String> kafkaProducer() {
//        logger.info("====== [TEST]: 启动SQL重放kafka生产客户端：brokers:{}, topic: {}", brokers, topic);
//        return new KafkaProducer<>(producerConfigs());
//    }


    @Override
    public void afterPropertiesSet() throws Exception {
        if (appId != null && !appId.trim().isEmpty()) {
            topic = KafkaConfig.TOPIC_PREFIX + appId;
            if(PRODUCER.equalsIgnoreCase(clientType)){
                logger.info("====== [TEST]: 启动SQL重放kafka生产客户端：brokers:{}, topic: {}", brokers, topic);
                kafkaProducer = new KafkaProducer<>(producerConfigs());
            }
        } else {
            logger.error("sql重放，未配置app.id");
        }

    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getBrokers() {
        return brokers;
    }

    public void setBrokers(String brokers) {
        this.brokers = brokers;
    }

    public String getClientType() {
        return clientType;
    }

    public void setClientType(String clientType) {
        this.clientType = clientType;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }


    public KafkaProducer<String, String> getKafkaProducer() {
        return kafkaProducer;
    }

    public void setKafkaProducer(KafkaProducer<String, String> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }
}
