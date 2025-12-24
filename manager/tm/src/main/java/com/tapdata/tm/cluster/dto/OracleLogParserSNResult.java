package com.tapdata.tm.cluster.dto;

import lombok.Data;

import java.util.Map;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/11/14 09:41 Create
 * @description
 */
@Data
public class OracleLogParserSNResult {
    DataInfo data;

    String message;
    String status;

    @Data
    public static class DataInfo {
        Integer days;
        String issueDate;
        Integer modules;
        Integer nHosts;
        String oraVersion;
        String platForm;
        String serverId;
        Long timestamp;
        String user;

        public static DataInfo parse(Object mapObj) {
            DataInfo info = new DataInfo();
            if (mapObj instanceof Map<?,?> map) {
                info.setDays(parseInt(map.get("days")));
                info.setIssueDate(String.valueOf(map.get("issueDate")));
                info.setModules(parseInt(map.get("modules")));
                info.setNHosts(parseInt(map.get("nHosts")));
                info.setOraVersion(String.valueOf(map.get("oraVersion")));
                info.setPlatForm(String.valueOf(map.get("platForm")));
                info.setServerId(String.valueOf(map.get("serverId")));
                info.setTimestamp(parseLong(map.get("timestamp")));
                info.setUser(String.valueOf(map.get("user")));
            }
            return info;
        }

        public static Integer parseInt(Object obj) {
            if (obj instanceof Number num) {
                return num.intValue();
            } else if (obj instanceof String str) {
                try {
                    return Integer.parseInt(str);
                } catch (NumberFormatException e) {
                    return null;
                }
            }
            return null;
        }

        public static Long parseLong(Object obj) {
            if (obj instanceof Number num) {
                return num.longValue();
            } else if (obj instanceof String str) {
                try {
                    return Long.parseLong(str);
                } catch (NumberFormatException e) {
                    return null;
                }
            }
            return null;
        }
    }
}
