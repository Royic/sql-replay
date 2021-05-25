package com.replay.sql;

import org.apache.ibatis.type.JdbcType;

public class SqlParam {
    private Object value;
    private SqlDataType dataType;
    private JdbcType jdbcType;

    public SqlParam(Object value, SqlDataType dataType, JdbcType jdbcType) {
        this.value = value;
        this.dataType = dataType;
        this.jdbcType = jdbcType;
    }

    public SqlParam() {
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object param) {
        this.value = param;
    }

    public JdbcType getJdbcType() {
        return jdbcType;
    }

    public void setJdbcType(JdbcType jdbcType) {
        this.jdbcType = jdbcType;
    }

    public SqlDataType getDataType() {
        return dataType;
    }

    public void setDataType(SqlDataType dataType) {
        this.dataType = dataType;
    }

    @Override
    public String toString() {
        return "SqlParam{" +
                "value=" + value +
                ", jdbcType=" + jdbcType +
                ", dataType='" + dataType + '\'' +
                '}';
    }
}
