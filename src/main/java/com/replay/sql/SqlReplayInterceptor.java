package com.replay.sql;

import com.alibaba.fastjson.JSON;
import org.apache.ibatis.executor.statement.StatementHandler;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.mapping.ParameterMode;
import org.apache.ibatis.plugin.*;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.type.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author lile
 * SQL重放拦截器，用于阿里云迁移至腾讯云时测试使用
 */
@Conditional(SqlReplayProducerEnable.class)
@Component
@Intercepts({@Signature(type = StatementHandler.class, method = "parameterize", args = {Statement.class})})
public class SqlReplayInterceptor implements Interceptor, InitializingBean {
    private static final Logger logger = LoggerFactory.getLogger(SqlReplayInterceptor.class);

    private static final ExecutorService executor = new ThreadPoolExecutor(5, 5,
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>(), new ThreadFactoryBuilder().setNameFormat("sql-replay-producer-").build());
    @Autowired(required = false)
    private SqlSessionFactory sqlSessionFactory;
    @Resource
    private KafkaConfig kafkaConfig;

    private KafkaProducer<String, String> kafkaProducer;


    @Override
    public Object intercept(Invocation invocation) throws Throwable {
        if (!KafkaConfig.PRODUCER.equalsIgnoreCase(kafkaConfig.getClientType()) || kafkaProducer == null) {
            return invocation.proceed();
        }
        try {
            StatementHandler statementHandler = (StatementHandler) invocation.getTarget();
            final BoundSql boundSql = statementHandler.getBoundSql();
            final Long threadId = Thread.currentThread().getId();
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    sendSql(boundSql, threadId);
                }
            });

        } catch (Throwable e) {
            logger.error("SQL重放拦截器异常", e);
        }
        return invocation.proceed();
    }

    private void sendSql(BoundSql boundSql, Long threadId) {
        String sql = null;
        try {
            if (kafkaConfig.getTopic() == null) {
                return;
            }
            sql = boundSql.getSql();
            SqlParam[] params = getParams(boundSql);
            SqlWrapper sqlWrapper = new SqlWrapper(sql, params, threadId);
            if (kafkaProducer != null) {
                logger.debug("发送SQL-----------------:{}, 参数：{}, threadId: {}, topic: {}", sql, params, threadId, kafkaConfig.getTopic());
                kafkaProducer.send(new ProducerRecord<>(kafkaConfig.getTopic(), String.valueOf(threadId), JSON.toJSONString(sqlWrapper)));
            }
        } catch (Throwable e) {
            logger.error("SQL重放发送异常, 原SQL:{}", sql, e);
        }
    }


    public SqlParam[] getParams(BoundSql boundSql) throws SQLException {
        List<ParameterMapping> parameterMappings = boundSql.getParameterMappings();
        Object parameterObject = boundSql.getParameterObject();
        SqlParam[] paramValues = new SqlParam[]{};
        if (parameterMappings != null) {
            paramValues = new SqlParam[parameterMappings.size()];
            for (int i = 0; i < parameterMappings.size(); ++i) {
                ParameterMapping parameterMapping = parameterMappings.get(i);
                if (parameterMapping.getMode() != ParameterMode.OUT) {
                    String propertyName = parameterMapping.getProperty();
                    Object value;
                    if (boundSql.hasAdditionalParameter(propertyName)) {
                        value = boundSql.getAdditionalParameter(propertyName);
                    } else if (parameterObject == null) {
                        value = null;
                    } else if (sqlSessionFactory.getConfiguration().getTypeHandlerRegistry().hasTypeHandler(parameterObject.getClass())) {
                        value = parameterObject;
                    } else {
                        MetaObject metaObject = sqlSessionFactory.getConfiguration().newMetaObject(parameterObject);
                        value = metaObject.getValue(propertyName);
                    }

                    TypeHandler typeHandler = parameterMapping.getTypeHandler();
                    JdbcType jdbcType = parameterMapping.getJdbcType();
                    if (value == null && jdbcType == null) {
                        jdbcType = sqlSessionFactory.getConfiguration().getJdbcTypeForNull();
                    }
                    paramValues[i] = getParameterValue(typeHandler, value, jdbcType);

                }
            }
        }
        return paramValues;
    }

    private SqlParam getParameterValue(TypeHandler typeHandler, Object parameter, JdbcType jdbcType) {
        if (parameter == null) {
            return null;
        } else {
            return getNonNullParameterValue(typeHandler, parameter, jdbcType);
        }
    }

    private SqlParam getNonNullParameterValue(TypeHandler typeHandler, Object parameter, JdbcType jdbcType) {
        if (typeHandler instanceof ArrayTypeHandler) {
            return new SqlParam(parameter, SqlDataType.Array, jdbcType);//setArray
        } else if (typeHandler instanceof BigDecimalTypeHandler) {
            return new SqlParam(parameter, SqlDataType.BigDecimal, jdbcType);
//            return parameter;// setBigdecimal
        } else if (typeHandler instanceof BigIntegerTypeHandler) {
            return new SqlParam(new BigDecimal((BigInteger) parameter), SqlDataType.BigDecimal, jdbcType);
//            return new BigDecimal((BigInteger) parameter);// setBigdecimal
        } else if (typeHandler instanceof BlobByteObjectArrayTypeHandler) {
            return new SqlParam(parameter, SqlDataType.Blob, jdbcType);

//             ByteArrayInputStream bis = new ByteArrayInputStream(convertToPrimitiveArray((Byte[]) parameter));
        } else if (typeHandler instanceof BlobTypeHandler) {
            return new SqlParam(parameter, SqlDataType.Blob, jdbcType);
//            ByteArrayInputStream bis = new ByteArrayInputStream((byte[]) parameter);
            //todo
//            return parameter;// ps.setBinaryStream(i, bis, parameter.length);

        } else if (typeHandler instanceof BooleanTypeHandler) {
            return new SqlParam(parameter, SqlDataType.Boolean, jdbcType);
//            return parameter;//setBoolean;

        } else if (typeHandler instanceof ByteArrayTypeHandler) {
            return new SqlParam(parameter, SqlDataType.ByteArray, jdbcType);
//            return parameter;// ps.setBytes(i, parameter);
        } else if (typeHandler instanceof ByteObjectArrayTypeHandler) {
            return new SqlParam(parameter, SqlDataType.ByteArray, jdbcType);
//            return convertToPrimitiveArray((Byte[]) parameter);
//            ps.setBytes(i, ByteArrayUtils.convertToPrimitiveArray(parameter));

        } else if (typeHandler instanceof ByteTypeHandler) {
            return new SqlParam(parameter, SqlDataType.Byte, jdbcType);
//            return parameter;//  ps.setByte(i, parameter);
        } else if (typeHandler instanceof CharacterTypeHandler) {
            return new SqlParam(parameter.toString(), SqlDataType.String, jdbcType);
//            return parameter.toString();//  ps.setString(i, parameter.toString());
        } else if (typeHandler instanceof ClobTypeHandler) {
            return new SqlParam(parameter, SqlDataType.Clob, jdbcType);
//            StringReader reader = new StringReader((String)parameter);
            //todo;
//            return parameter;//  //ps.setCharacterStream(i, reader, parameter.length());
        } else if (typeHandler instanceof DateOnlyTypeHandler) {
            return new SqlParam(parameter, SqlDataType.Date, jdbcType);
            //todo
//            return parameter;//   ps.setDate(i, new java.sql.Date(parameter.getTime()));
        } else if (typeHandler instanceof DateTypeHandler) {
            return new SqlParam(parameter, SqlDataType.Timestamp, jdbcType);
            //todo
//            return parameter;//ps.setTimestamp(i, new Timestamp(parameter.getTime()));
        } else if (typeHandler instanceof DoubleTypeHandler) {
            return new SqlParam(parameter, SqlDataType.Double, jdbcType);
//            return parameter;//  ps.setDouble(i, parameter);
        } else if (typeHandler instanceof EnumOrdinalTypeHandler) {
            return new SqlParam(((Enum) parameter).ordinal(), SqlDataType.Integer, jdbcType);
            //todo;
//            return ((Enum)parameter).ordinal();//    ps.setInt(i, parameter.ordinal());
        } else if (typeHandler instanceof EnumTypeHandler) {
            //todo;
//            if (jdbcType == null) {
//                ps.setString(i, parameter.name());
//            } else {
//                ps.setObject(i, parameter.name(), jdbcType.TYPE_CODE);
//            }

            if (jdbcType == null) {
                return new SqlParam(((Enum) parameter).name(), SqlDataType.String, jdbcType);
//                return ((Enum)parameter).name();
            } else {
                return new SqlParam(((Enum) parameter).name(), SqlDataType.Object, jdbcType);
//                return ((Enum)parameter).name();
            }
        } else if (typeHandler instanceof FloatTypeHandler) {
            return new SqlParam(parameter, SqlDataType.Float, jdbcType);
//            return parameter;//  ps.setFloat(i, parameter);
        } else if (typeHandler instanceof IntegerTypeHandler) {
            return new SqlParam(parameter, SqlDataType.Integer, jdbcType);
//            return parameter;//  ps.setInt(i, parameter);
        } else if (typeHandler instanceof LongTypeHandler) {
            return new SqlParam(parameter, SqlDataType.Long, jdbcType);
//            return parameter;//  ps.setLong(i, parameter);
        } else if (typeHandler instanceof NClobTypeHandler) {
            return new SqlParam(parameter, SqlDataType.Clob, jdbcType);
//            StringReader reader = new StringReader((String)parameter);
            //todo;
//            return parameter;//  //ps.setCharacterStream(i, reader, parameter.length());
        } else if (typeHandler instanceof NStringTypeHandler) {
            return new SqlParam(parameter, SqlDataType.String, jdbcType);
//            return parameter;//    ps.setString(i, parameter);
        } else if (typeHandler instanceof ObjectTypeHandler) {
            return new SqlParam(parameter, SqlDataType.Object, jdbcType);
//            return parameter;//   ps.setObject(i, parameter);
        } else if (typeHandler instanceof ShortTypeHandler) {
            return new SqlParam(parameter, SqlDataType.Short, jdbcType);
//            return parameter;//    ps.setShort(i, parameter);
        } else if (typeHandler instanceof SqlDateTypeHandler) {
            return new SqlParam(parameter, SqlDataType.Date, jdbcType);
//            return parameter;//    ps.setDate(i, parameter);
        } else if (typeHandler instanceof SqlTimestampTypeHandler) {
            return new SqlParam(parameter, SqlDataType.Timestamp, jdbcType);
//            return parameter;//     ps.setTimestamp(i, parameter);
        } else if (typeHandler instanceof SqlTimeTypeHandler) {
            return new SqlParam(parameter, SqlDataType.Time, jdbcType);
//            return parameter;//    ps.setTime(i, parameter);
        } else if (typeHandler instanceof StringTypeHandler) {
            return new SqlParam(parameter, SqlDataType.String, jdbcType);
//            return parameter;//      ps.setString(i, parameter);
        } else if (typeHandler instanceof TimeOnlyTypeHandler) {
            return new SqlParam(parameter, SqlDataType.Time, jdbcType);
//            return parameter;//    ps.setTime(i, new Time(parameter.getTime()));
        } else if (typeHandler instanceof UnknownTypeHandler) {
            TypeHandler handler = this.resolveTypeHandler(parameter, jdbcType);
            return getParameterValue(handler, parameter, jdbcType);
        }
        return null;
    }

    private TypeHandler resolveTypeHandler(Object parameter, JdbcType jdbcType) {
        Object handler;
        if (parameter == null) {
            handler = new ObjectTypeHandler();
        } else {
            handler = sqlSessionFactory.getConfiguration().getTypeHandlerRegistry().getTypeHandler(parameter.getClass(), jdbcType);
            if (handler == null || handler instanceof UnknownTypeHandler) {
                handler = new ObjectTypeHandler();
            }
        }

        return (TypeHandler) handler;
    }


    @Override
    public Object plugin(Object o) {
        if (o instanceof StatementHandler) {
            return Plugin.wrap(o, this);
        } else {
            return o;
        }
    }

    @Override
    public void setProperties(Properties properties) {
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (sqlSessionFactory == null) {
            logger.error("未找到启用的sqlSessionFactory");
        } else {
            try {
                sqlSessionFactory.getConfiguration().addInterceptor(this);
            } catch (Exception e) {
                logger.error("====== [TEST]， 设置SQL重放拦截器失败，{}", e.getMessage());
            }
        }
        if (kafkaConfig != null && kafkaConfig.getClientType() != null && !kafkaConfig.getClientType().trim().isEmpty()) {
            kafkaProducer = kafkaConfig.getKafkaProducer();
            if (kafkaProducer == null) {
                logger.error("====== [TEST]： kafka生产端未初始化");
            }
            logger.info("====== [TEST]: SQL重放已启用, 客户端类型为：{}", kafkaConfig.getClientType());
        }

    }
}
