package com.replay.sql;


import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 * 如果存在配置：${sql.replay.client.type}， 则连接kafka,启用sql同步逻辑
 */
public class SqlReplayProducerEnable implements Condition {
    @Override
    public boolean matches(ConditionContext conditionContext, AnnotatedTypeMetadata annotatedTypeMetadata) {
        Environment environment = conditionContext.getEnvironment();
        String clientType = environment.getProperty("sql.replay.client.type");
        return "producer".equalsIgnoreCase(clientType);
    }
}
