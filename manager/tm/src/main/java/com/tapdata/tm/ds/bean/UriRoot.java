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
package com.tapdata.tm.ds.bean;


import lombok.*;

import java.util.Map;

/**
 * @Author: Zed
 * @Date: 2021/6/30
 * @Description:
 */
@AllArgsConstructor
@Getter
@Setter
@Builder
public class UriRoot {
  private String schema;
  private String userName;
  private String password;
  private Integer port;
  private String host;
  private String hosts;
  private Map<String, Integer> clusters;
  private String resource;
  private Map<String, String> params;
  private String addParams;
}

