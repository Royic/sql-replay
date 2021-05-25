package com.replay.sql;

import com.alibaba.fastjson.JSON;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.*;
import java.text.ParseException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.*;

/**
 * @lile 用于数据迁移的SQL重放的消费者
 */

@Conditional(SqlReplayConsumerEnable.class)
@Component
public class SqlReplayConsumer {
    private static final Logger logger = LoggerFactory.getLogger(SqlReplayConsumer.class);
    private static final ExecutorService START_THREAD = new ThreadPoolExecutor(1, 1,
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>(), new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName("sql-replay-consumer-start");
            return t;
        }
    });
    private static final SqlReplayExecutor REPLAY_EXECUTOR = new SqlReplayExecutor();
    @Autowired
    private SqlSessionFactory sqlSessionFactory;
    @Resource(name = "consumerConfigs")
    private Map<String, Object> consumerConfigs;
    @Autowired
    private KafkaConfig kafkaConfig;

    @PostConstruct
    public void listen() {
        if (KafkaConfig.CONSUMER.equalsIgnoreCase(kafkaConfig.getClientType())) {
            if (kafkaConfig.getTopic() == null) {
                logger.error("SQL重放，未配置有效的topic，停止消费端启动");
                return;
            }
            START_THREAD.execute(new Runnable() {
                @Override
                public void run() {
                    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfigs);
                    consumer.subscribe(Collections.singletonList(kafkaConfig.getTopic()));
                    logger.info("====== [TEST]: SQL重放消费端已启动，topic: {}", kafkaConfig.getTopic());
                    while (true) {
                        ConsumerRecords<String, String> records = consumer.poll(3000);
                        for (ConsumerRecord<String, String> cr : records) {
                            consume(cr.value());
                        }
                    }
                }
            });

        }
    }

    private void consume(final String message) {
        try {
            if (message == null || message.trim().isEmpty()) {
                logger.error("获取的消息为空");
                return;
            }
            final SqlWrapper sqlWrapper = JSON.parseObject(message, SqlWrapper.class);
            if (sqlWrapper != null) {
                REPLAY_EXECUTOR.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            logger.debug("Received Sql: {}", message);
                            replay(sqlWrapper);
                        } catch (Exception e) {
                            logger.error("重放SQL执行失败，SQL信息：{}", message, e);
                        }
                    }
                }, sqlWrapper.getThreadId());
            }
        } catch (Throwable e) {
            logger.error("重放SQL执行失败，SQL信息：{}", message, e);
        }
    }

    private void replay(SqlWrapper sqlWrapper) throws Exception {
        Connection connection = null;
        try {
            connection = sqlSessionFactory.openSession().getConnection();
            SqlParam[] params = sqlWrapper.getParams();
            PreparedStatement statement = connection.prepareStatement(sqlWrapper.getSql());
            if (params != null && params.length != 0) {
                for (int i = 0; i < params.length; i++) {
                    SqlParam param = params[i];
                    int j = i + 1;
                    if (param == null || param.getValue() == null) {
                        statement.setNull(j, Types.NULL);
                    } else {
                        Object value = param.getValue();
                        switch (param.getDataType()) {
                            case Long:
                                statement.setLong(j, Long.valueOf(value.toString()));
                                break;
                            case Integer:
                                statement.setInt(j, Integer.valueOf(value.toString()));
                                break;
                            case Byte:
                                statement.setByte(j, Byte.valueOf(value.toString()));
                                break;
                            case Double:
                                statement.setDouble(j, ((BigDecimal) value).doubleValue());
                                break;
                            case Float:
                                statement.setFloat(j, ((BigDecimal) value).floatValue());
                                break;
                            case Boolean:
                                statement.setBoolean(j, (Boolean) value);
                                break;
                            case String:
                                statement.setString(j, (String) value);
                                break;
                            case Short:
                                statement.setShort(j, (Short) value);
                                break;
                            case Date:
                                statement.setDate(j, toDate((Long) value));
                                break;
                            case Timestamp:
                                statement.setTimestamp(j, toTimestamp((Long) value));
                                break;
                            case Time:
                                statement.setTime(j, toTime((Long) value));
                                break;
                            case ByteArray:
                                statement.setBytes(j, convertToPrimitiveArray((Byte[]) value));
                                break;
                            case Blob:
                                ByteArrayInputStream bis = new ByteArrayInputStream(convertToPrimitiveArray((Byte[]) value));
                                statement.setBinaryStream(j, bis);
                                break;
                            case Clob:
                                StringReader reader = new StringReader((String) value);
                                statement.setCharacterStream(j, reader);
                                break;
                            case BigDecimal:
                                statement.setBigDecimal(j, BigDecimal.valueOf(Double.parseDouble(value.toString())));
                                break;
                            case Object:
                                statement.setObject(j, value);
                                break;
                            default:

                        }
                    }
                }
            }
            statement.execute();
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    logger.error("SQL重放关闭连接失败", e);
                }
            }
        }
    }


    private Date toDate(Long value) throws ParseException {
//        return new Date(DateUtils.parseDate(str, "yyyy-MM-dd", "yyyy-MM-dd HH:mm:ss.SSS").getTime());
        return new Date(value);
    }

    private Timestamp toTimestamp(Long value) throws ParseException {
        return new Timestamp(value);
    }

    private Time toTime(Long value) throws ParseException {
        return new Time(value);
    }


    static byte[] convertToPrimitiveArray(Byte[] objects) {
        byte[] bytes = new byte[objects.length];

        for (int i = 0; i < objects.length; ++i) {
            Byte b = objects[i];
            bytes[i] = b;
        }

        return bytes;
    }


}
