package com.replay.sql;

import java.util.Arrays;

public class SqlWrapper {
    private String sql;
    private SqlParam[] params;
    private Long threadId;

    public SqlWrapper(String sql, SqlParam[] params, Long threadId) {
        this.sql = sql;
        this.params = params;
        this.threadId = threadId;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public SqlParam[] getParams() {
        return params;
    }

    public void setParams(SqlParam[] params) {
        this.params = params;
    }

    public Long getThreadId() {
        return threadId;
    }

    public void setThreadId(Long threadId) {
        this.threadId = threadId;
    }

    @Override
    public String toString() {
        return "SqlWrapper{" +
                "sql='" + sql + '\'' +
                ", params=" + Arrays.toString(params) +
                ", threadId=" + threadId +
                '}';
    }
}
