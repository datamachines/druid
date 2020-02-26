/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.server;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserDefaultVisitor;
import net.sf.jsqlparser.parser.CCJSqlParserTreeConstants;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.parser.SimpleNode;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.Query;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;

public class RequestLogLine
{
  private static final Joiner JOINER = Joiner.on("\t");

  private final Query query;
  private final String sql;
  private final Map<String, Object> sqlQueryContext;
  private final HashSet<String> sqlColumns;
  private final DateTime timestamp;
  private final String remoteAddr;
  private final QueryStats queryStats;

  private RequestLogLine(
      @Nullable Query query,
      @Nullable String sql,
      @Nullable Map<String, Object> sqlQueryContext,
      @Nullable HashSet<String> sqlColumns,
      DateTime timestamp,
      @Nullable String remoteAddr,
      QueryStats queryStats
  )
  {
    this.query = query;
    this.sql = sql;
    this.sqlQueryContext = sqlQueryContext != null ? sqlQueryContext : ImmutableMap.of();
    this.sqlColumns = (sqlColumns != null) ? sqlColumns : getColumns();
    this.timestamp = Preconditions.checkNotNull(timestamp, "timestamp");
    this.remoteAddr = StringUtils.nullToEmptyNonDruidDataString(remoteAddr);
    this.queryStats = Preconditions.checkNotNull(queryStats, "queryStats");
  }

  public static RequestLogLine forNative(Query query, DateTime timestamp, String remoteAddr, QueryStats queryStats)
  {
    return new RequestLogLine(query, null, null, null, timestamp, remoteAddr, queryStats);
  }

  public static RequestLogLine forSql(
      String sql,
      Map<String, Object> sqlQueryContext,
      HashSet<String> sqlColumns,
      DateTime timestamp,
      String remoteAddr,
      QueryStats queryStats
  )
  {
    return new RequestLogLine(null, sql, sqlQueryContext, sqlColumns, timestamp, remoteAddr, queryStats);
  }

  public String getNativeQueryLine(ObjectMapper objectMapper) throws JsonProcessingException
  {
    return JOINER.join(
        Arrays.asList(
            timestamp,
            remoteAddr,
            objectMapper.writeValueAsString(query),
            objectMapper.writeValueAsString(queryStats)
        )
    );
  }

  public String getSqlQueryLine(ObjectMapper objectMapper) throws JsonProcessingException
  {
    return JOINER.join(
        Arrays.asList(
            timestamp,
            remoteAddr,
            "",
            objectMapper.writeValueAsString(queryStats),
            objectMapper.writeValueAsString(ImmutableMap.of("query", sql, "context", sqlQueryContext)),
            objectMapper.writeValueAsString(sqlColumns)
        )
    );
  }

  @Nullable
  @JsonProperty("query")
  public Query getQuery()
  {
    return query;
  }

  @Nullable
  @JsonProperty("sql")
  public String getSql()
  {
    return sql;
  }

  @Nullable
  @JsonProperty
  public Map<String, Object> getSqlQueryContext()
  {
    return sqlQueryContext;
  }

  @JsonProperty("timestamp")
  public DateTime getTimestamp()
  {
    return timestamp;
  }

  @Nullable
  @JsonProperty("remoteAddr")
  public String getRemoteAddr()
  {
    return remoteAddr;
  }

  @JsonProperty("queryStats")
  public QueryStats getQueryStats()
  {
    return queryStats;
  }

  @JsonProperty("sqlColumns")
  public HashSet<String> getColumns()
  {
    HashSet<String> columns = new HashSet<>();

    try {
      if (getSql() != null) {
        SimpleNode node = (SimpleNode) CCJSqlParserUtil.parseAST(getSql());
        node.jjtAccept(new CCJSqlParserDefaultVisitor() {
          @Override
          public Object visit(SimpleNode node, Object data)
          {
            if (node.getId() == CCJSqlParserTreeConstants.JJTCOLUMN) {
              if (node.jjtGetValue().toString() != null) {
                columns.add(node.jjtGetValue().toString());
              }

              return super.visit(node, data);
            } else {
              return super.visit(node, data);
            }
          }
        }, null);
      }
    }
    catch (JSQLParserException ignored) {
    }
    return columns;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RequestLogLine)) {
      return false;
    }
    RequestLogLine that = (RequestLogLine) o;
    return Objects.equals(query, that.query) &&
           Objects.equals(sql, that.sql) &&
           Objects.equals(sqlQueryContext, that.sqlQueryContext) &&
           Objects.equals(sqlColumns, that.sqlColumns) &&
           Objects.equals(timestamp, that.timestamp) &&
           Objects.equals(remoteAddr, that.remoteAddr) &&
           Objects.equals(queryStats, that.queryStats);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(query, sql, sqlQueryContext, sqlColumns, timestamp, remoteAddr, queryStats);
  }

  @Override
  public String toString()
  {
    return "RequestLogLine{" +
           "query=" + query +
           ", sql='" + sql + '\'' +
           ", sqlQueryContext=" + sqlQueryContext +
           ", sqlColumns=" + sqlColumns +
           ", timestamp=" + timestamp +
           ", remoteAddr='" + remoteAddr + '\'' +
           ", queryStats=" + queryStats +
           '}';
  }
}
