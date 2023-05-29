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

package com.tapdata.tm.ds.utils;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.DataSourceEnum;
import com.tapdata.tm.utils.AES256Util;
import org.apache.commons.lang3.StringUtils;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

/**
 * @Author: Zed
 * @Date: 2021/6/30
 * @Description:
 */
public class UriRootConvertUtils {

  private final static String mongoPrefix = "mongodb://";
  public final static String mongoHiddenPwd = "*********";

  public static String hidePassword(String uri) {
    if (StringUtils.isBlank(uri)) {
      throw new BizException("Datasource.MongoUriIsBlank", "Uri is blank.");
    }

    uri = uri.trim();

    int indexStart = uri.indexOf("://");
    int indexEnd = uri.indexOf("@");
    if (indexEnd == -1) {
      return uri;
    }

    String userNameAndPassword = uri.substring(indexStart + 3, indexEnd);
    int indexPasswd = userNameAndPassword.indexOf(":");
    return uri.substring(0, indexStart + 4 + indexPasswd) + mongoHiddenPwd + uri.substring(indexEnd);
  }

  public static String constructUri(DataSourceConnectionDto connectionDto) {
    StringBuilder uri = new StringBuilder(mongoPrefix);
    if (DataSourceEnum.isMongoDB(connectionDto.getDatabase_type())) {
      if (StringUtils.isNotBlank(connectionDto.getDatabase_username())) {
        try {
          String encodeUserName = URLEncoder.encode(connectionDto.getDatabase_username(), "UTF-8");
          uri.append(encodeUserName);

          String plainPassword = connectionDto.getPlain_password();
          if (StringUtils.isBlank(plainPassword)) {
            plainPassword = AES256Util.Aes256Decode(connectionDto.getDatabase_password());
          }

          String encodePassword = URLEncoder.encode(plainPassword, "UTF-8");
          uri.append(":").append(encodePassword).append("@");
        } catch (UnsupportedEncodingException e) {
          throw new BizException("SystemError");
        }
      }

      uri.append(connectionDto.getDatabase_host()).append("/");
      uri.append(connectionDto.getDatabase_name());
      if (StringUtils.isNotBlank(connectionDto.getAdditionalString())) {
        uri.append("?").append(connectionDto.getAdditionalString());
      }

    }
    return uri.toString();
  }

}
