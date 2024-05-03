/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.calcite.adapter.couchdb;

import com.google.common.collect.ImmutableSet;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import org.lightcouch.CouchDbClient;
import org.lightcouch.CouchDbProperties;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class CouchSchemaFactory implements SchemaFactory {
  public CouchSchemaFactory() {
    super();
  }

  private static final int DEFAULT_COUCHDB_PORT = 5984;

  // CouchSchema 생성
  @Override
  public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
    Map<String, Object> info_map = getInfo(operand);
    return new CouchSchema(info_map, getPort(operand));
  }

  // model 파일의 데이터로 map을 생성
  // 스키마 생성 시 사용
  private Map<String, Object> getInfo(Map<String, Object> map) {
    Map<String, Object> ret = new HashMap<>();

    final String host = (String) map.get("host");
    final String username = (String) map.get("username");
    final String password = (String) map.get("password");
    final String protocol = (String) map.get("protocol");
    final String database = (String) map.get("database");

    ret.put("host", host);
    ret.put("username", username);
    ret.put("password", password);
    ret.put("protocol", protocol);
    ret.put("database", database);
    return ret;
  }

  // 포트를 int 형으로 변경
  private int getPort(Map<String, Object> map) {
    if (map.containsKey("port")) {
      Object port = map.get("port");
      if(port instanceof String)
        return Integer.parseInt((String) map.get("port"));
      return (int) port;
    }

    return DEFAULT_COUCHDB_PORT;
  }
}
