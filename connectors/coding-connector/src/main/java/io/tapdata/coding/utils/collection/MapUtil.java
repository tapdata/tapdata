package io.tapdata.coding.utils.collection;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import io.tapdata.coding.enums.Constants;

import java.util.Map;
import java.util.StringJoiner;

public class MapUtil<K,V> {
    public static MapUtil create(){
        return new MapUtil<>();
    }
    public Integer hashCode(Map<K,V> map){
        if (null == map) return -1;
        return toString(map).hashCode();
    }

    private String toString(Map<K,V> map){
        if (null == map) return "";
        if (map.size()==0) return map.toString();
        JSONObject jsonObject = JSONUtil.parseObj(map);
        return jsonObject.toString();
    }
    public static String fileds = "{\n" +
            "  \"Issue\": {\n" +
            "    \"ParentType\": \"MISSION\",\n" +
            "    \"Code\": 4,\n" +
            "    \"Type\": \"SUB_TASK\",\n" +
            "    \"Name\": \"确定用户反馈渠道, 建议用简版论坛, 有价值沉淀\",\n" +
            "    \"Description\": \"\",\n" +
            "    \"IterationId\": 0,\n" +
            "    \"IssueStatusId\": 1587660,\n" +
            "    \"IssueStatusName\": \"未开始\",\n" +
            "    \"IssueStatusType\": \"TODO\",\n" +
            "    \"Priority\": \"2\",\n" +
            "    \"Epic\": {\n" +
            "      \"Code\": 0,\n" +
            "      \"Type\": \"\",\n" +
            "      \"Name\": \"\",\n" +
            "      \"IssueStatusId\": 0,\n" +
            "      \"IssueStatusName\": \"\",\n" +
            "      \"Priority\": \"\",\n" +
            "      \"Assignee\": {\n" +
            "        \"Id\": 0,\n" +
            "        \"Status\": 0,\n" +
            "        \"Avatar\": \"\",\n" +
            "        \"Name\": \"\",\n" +
            "        \"Email\": \"\",\n" +
            "        \"TeamId\": 0,\n" +
            "        \"Phone\": \"\",\n" +
            "        \"GlobalKey\": \"\",\n" +
            "        \"TeamGlobalKey\": \"\"\n" +
            "      }\n" +
            "    },\n" +
            "    \"Assignee\": {\n" +
            "      \"Id\": 0,\n" +
            "      \"Status\": 0,\n" +
            "      \"Avatar\": \"\",\n" +
            "      \"Name\": \"\",\n" +
            "      \"Email\": \"\",\n" +
            "      \"TeamId\": 0,\n" +
            "      \"Phone\": \"\",\n" +
            "      \"GlobalKey\": \"\",\n" +
            "      \"TeamGlobalKey\": \"\"\n" +
            "    },\n" +
            "    \"StartDate\": 0,\n" +
            "    \"DueDate\": 0,\n" +
            "    \"WorkingHours\": 0,\n" +
            "    \"Creator\": {\n" +
            "      \"Id\": 8054404,\n" +
            "      \"Status\": 1,\n" +
            "      \"Avatar\": \"https://coding-net-production-static-ci.codehub.cn/WM-TEXT-AVATAR-eIKPrrFIbZvWEBGUurtc.jpg\",\n" +
            "      \"Name\": \"Berry\",\n" +
            "      \"Email\": \"\",\n" +
            "      \"TeamId\": 0,\n" +
            "      \"Phone\": \"\",\n" +
            "      \"GlobalKey\": \"\",\n" +
            "      \"TeamGlobalKey\": \"\"\n" +
            "    },\n" +
            "    \"StoryPoint\": \"\",\n" +
            "    \"CreatedAt\": 1615222152000,\n" +
            "    \"UpdatedAt\": 1615226431000,\n" +
            "    \"CompletedAt\": 0,\n" +
            "    \"ProjectModule\": {\n" +
            "      \"Id\": 0,\n" +
            "      \"Name\": \"\"\n" +
            "    },\n" +
            "    \"Watchers\": [\n" +
            "      {\n" +
            "        \"Id\": 8054404,\n" +
            "        \"Status\": 1,\n" +
            "        \"Avatar\": \"https://coding-net-production-static-ci.codehub.cn/WM-TEXT-AVATAR-eIKPrrFIbZvWEBGUurtc.jpg\",\n" +
            "        \"Name\": \"Berry\",\n" +
            "        \"Email\": \"\",\n" +
            "        \"TeamId\": 0,\n" +
            "        \"Phone\": \"\",\n" +
            "        \"GlobalKey\": \"\",\n" +
            "        \"TeamGlobalKey\": \"\"\n" +
            "      }\n" +
            "    ],\n" +
            "    \"Labels\": [\n" +
            "      \n" +
            "    ],\n" +
            "    \"Files\": [\n" +
            "      \n" +
            "    ],\n" +
            "    \"RequirementType\": {\n" +
            "      \"Id\": 0,\n" +
            "      \"Name\": \"\"\n" +
            "    },\n" +
            "    \"DefectType\": {\n" +
            "      \"Id\": 0,\n" +
            "      \"Name\": \"\",\n" +
            "      \"IconUrl\": \"\"\n" +
            "    },\n" +
            "    \"CustomFields\": [\n" +
            "      \n" +
            "    ],\n" +
            "    \"ThirdLinks\": [\n" +
            "      \n" +
            "    ],\n" +
            "    \"SubTasks\": [\n" +
            "      \n" +
            "    ],\n" +
            "    \"Parent\": {\n" +
            "      \"Code\": 2,\n" +
            "      \"Type\": \"MISSION\",\n" +
            "      \"Name\": \"云版首页设计\",\n" +
            "      \"IssueStatusId\": 1587684,\n" +
            "      \"IssueStatusName\": \"已完成\",\n" +
            "      \"Priority\": \"2\",\n" +
            "      \"Assignee\": {\n" +
            "        \"Id\": 0,\n" +
            "        \"Status\": 0,\n" +
            "        \"Avatar\": \"\",\n" +
            "        \"Name\": \"\",\n" +
            "        \"Email\": \"\",\n" +
            "        \"TeamId\": 0,\n" +
            "        \"Phone\": \"\",\n" +
            "        \"GlobalKey\": \"\",\n" +
            "        \"TeamGlobalKey\": \"\"\n" +
            "      },\n" +
            "      \"IssueStatusType\": \"COMPLETED\",\n" +
            "      \"IssueTypeDetail\": {\n" +
            "        \"Id\": 0,\n" +
            "        \"Name\": \"\",\n" +
            "        \"IssueType\": \"\",\n" +
            "        \"Description\": \"\",\n" +
            "        \"IsSystem\": false\n" +
            "      }\n" +
            "    },\n" +
            "    \"Iteration\": {\n" +
            "      \"Code\": 0,\n" +
            "      \"Name\": \"\",\n" +
            "      \"Status\": \"\",\n" +
            "      \"Id\": 0\n" +
            "    },\n" +
            "    \"IssueTypeDetail\": {\n" +
            "      \"Id\": 104985,\n" +
            "      \"Name\": \"子工作项\",\n" +
            "      \"IssueType\": \"SUB_TASK\",\n" +
            "      \"Description\": \"在敏捷模式下，将一个事项拆分成更小的块。\",\n" +
            "      \"IsSystem\": true\n" +
            "    },\n" +
            "    \"IssueTypeId\": 104985\n" +
            "  }\n" +
            "}";

    public static String issue = "{\n" +
            "  \"Issue\": {\n" +
            "    \"Epic\": {\n" +
            "      \"Code\": 0,\n" +
            "      \"Type\": \"\",\n" +
            "      \"Name\": \"\",\n" +
            "      \"IssueStatusId\": 0,\n" +
            "      \"IssueStatusName\": \"\",\n" +
            "      \"Priority\": \"\",\n" +
            "      \"Assignee\": {\n" +
            "        \"Id\": 0,\n" +
            "        \"Status\": 0,\n" +
            "        \"Avatar\": \"\",\n" +
            "        \"Name\": \"\",\n" +
            "        \"Email\": \"\",\n" +
            "        \"TeamId\": 0,\n" +
            "        \"Phone\": \"\",\n" +
            "        \"GlobalKey\": \"\",\n" +
            "        \"TeamGlobalKey\": \"\"\n" +
            "      }\n" +
            "    },\n" +
            "    \"ParentType\": \"MISSION\",\n" +
            "    \"Code\": 4,\n" +
            "    \"Type\": \"SUB_TASK\",\n" +
            "    \"Name\": \"确定用户反馈渠道, 建议用简版论坛, 有价值沉淀\",\n" +
            "    \"Description\": \"dfsdfwefsdmfksfskjdnfjrefewfjwanfjdsnfjsdfks\",\n" +
            "    \"IterationId\": 0,\n" +
            "    \"IssueStatusId\": 1587660,\n" +
            "    \"IssueStatusName\": \"未开始\",\n" +
            "    \"IssueStatusType\": \"TODO\",\n" +
            "    \"Priority\": \"2\",\n" +
            "    \"Assignee\": {\n" +
            "      \"Id\": 0,\n" +
            "      \"Status\": 0,\n" +
            "      \"Avatar\": \"\",\n" +
            "      \"Name\": \"\",\n" +
            "      \"Email\": \"\",\n" +
            "      \"TeamId\": 0,\n" +
            "      \"Phone\": \"\",\n" +
            "      \"GlobalKey\": \"\",\n" +
            "      \"TeamGlobalKey\": \"\"\n" +
            "    },\n" +
            "    \"StartDate\": 0,\n" +
            "    \"DueDate\": 0,\n" +
            "    \"WorkingHours\": 0,\n" +
            "    \"Creator\": {\n" +
            "      \"Id\": 8054404,\n" +
            "      \"Status\": 1,\n" +
            "      \"Avatar\": \"https://coding-net-production-static-ci.codehub.cn/WM-TEXT-AVATAR-eIKPrrFIbZvWEBGUurtc.jpg\",\n" +
            "      \"Name\": \"Berry\",\n" +
            "      \"Email\": \"\",\n" +
            "      \"TeamId\": 0,\n" +
            "      \"Phone\": \"\",\n" +
            "      \"GlobalKey\": \"\",\n" +
            "      \"TeamGlobalKey\": \"\"\n" +
            "    },\n" +
            "    \"StoryPoint\": \"\",\n" +
            "    \"CreatedAt\": 1615222152000,\n" +
            "    \"UpdatedAt\": 1615226431000,\n" +
            "    \"CompletedAt\": 0,\n" +
            "    \"ProjectModule\": {\n" +
            "      \"Id\": 0,\n" +
            "      \"Name\": \"\"\n" +
            "    },\n" +
            "    \"Watchers\": [\n" +
            "      {\n" +
            "        \"Id\": 8054404,\n" +
            "        \"Status\": 1,\n" +
            "        \"Avatar\": \"https://coding-net-production-static-ci.codehub.cn/WM-TEXT-AVATAR-eIKPrrFIbZvWEBGUurtc.jpg\",\n" +
            "        \"Name\": \"Berry\",\n" +
            "        \"Email\": \"\",\n" +
            "        \"TeamId\": 0,\n" +
            "        \"Phone\": \"\",\n" +
            "        \"GlobalKey\": \"\",\n" +
            "        \"TeamGlobalKey\": \"\"\n" +
            "      }\n" +
            "    ],\n" +
            "    \"Labels\": [\n" +
            "      \n" +
            "    ],\n" +
            "    \"Files\": [\n" +
            "      \n" +
            "    ],\n" +
            "    \"RequirementType\": {\n" +
            "      \"Id\": 0,\n" +
            "      \"Name\": \"\"\n" +
            "    },\n" +
            "    \"DefectType\": {\n" +
            "      \"Id\": 0,\n" +
            "      \"Name\": \"\",\n" +
            "      \"IconUrl\": \"\"\n" +
            "    },\n" +
            "    \"CustomFields\": [\n" +
            "      \n" +
            "    ],\n" +
            "    \"ThirdLinks\": [\n" +
            "      \n" +
            "    ],\n" +
            "    \"SubTasks\": [\n" +
            "      \n" +
            "    ],\n" +
            "    \"Parent\": {\n" +
            "      \"Code\": 2,\n" +
            "      \"Type\": \"MISSION\",\n" +
            "      \"Name\": \"云版首页设计\",\n" +
            "      \"IssueStatusId\": 1587684,\n" +
            "      \"IssueStatusName\": \"已完成\",\n" +
            "      \"Priority\": \"2\",\n" +
            "      \"Assignee\": {\n" +
            "        \"Id\": 0,\n" +
            "        \"Status\": 0,\n" +
            "        \"Avatar\": \"\",\n" +
            "        \"Name\": \"\",\n" +
            "        \"Email\": \"\",\n" +
            "        \"TeamId\": 0,\n" +
            "        \"Phone\": \"\",\n" +
            "        \"GlobalKey\": \"\",\n" +
            "        \"TeamGlobalKey\": \"\"\n" +
            "      },\n" +
            "      \"IssueStatusType\": \"COMPLETED\",\n" +
            "      \"IssueTypeDetail\": {\n" +
            "        \"Id\": 0,\n" +
            "        \"Name\": \"\",\n" +
            "        \"IssueType\": \"\",\n" +
            "        \"Description\": \"\",\n" +
            "        \"IsSystem\": false\n" +
            "      }\n" +
            "    },\n" +
            "    \"Iteration\": {\n" +
            "      \"Code\": 0,\n" +
            "      \"Name\": \"\",\n" +
            "      \"Status\": \"\",\n" +
            "      \"Id\": 0\n" +
            "    },\n" +
            "    \"IssueTypeDetail\": {\n" +
            "      \"Id\": 104985,\n" +
            "      \"Name\": \"子工作项\",\n" +
            "      \"IssueType\": \"SUB_TASK\",\n" +
            "      \"Description\": \"在敏捷模式下，将一个事项拆分成更小的块。\",\n" +
            "      \"IsSystem\": true\n" +
            "    },\n" +
            "    \"IssueTypeId\": 104985\n" +
            "  }\n" +
            "}";

    public static void main(String[] args) {
//        Map<String,Object> map = new HashMap<>();
//        map.put("key1",new ArrayList<Object>(){{
//            add("va");
//            add("val");
//            add(1);
//        }});
//        map.put("key2",100000l);
//        map.put("key3",new HashMap<String,Object>(){{
//            put("key1-1",1);
//            put("key1-2","hello");
//        }});
//
//        System.out.println(map.toString());
//
//        JSONObject jsonObject = JSONUtil.parseObj(map);
//        System.out.println(jsonObject.toString());

        Map<String,Object> issueMap = JSONUtil.parseObj(issue);
        Map<String,Object> filedMap = JSONUtil.parseObj(fileds,false,true);
        System.out.println(CSVTitle(filedMap));
        String line = CSVLine(issueMap,filedMap);
        System.out.println(line);
    }
    public static Map<String,Object> fileds(){
        return JSONUtil.parseObj(fileds,false,true);
    }

    public static String CSVTitle(Map<String,Object> filedMap){
        StringJoiner joiner = new StringJoiner(Constants.SPLIT_CHAR);
        title(joiner,filedMap,null);
        return joiner.toString();
    }
    public static void title(StringJoiner joiner,Map<String,Object> filedMap,String key){
        for (final Map.Entry<String,Object> entry : filedMap.entrySet()){
            String entryKey = entry.getKey();
            Object entryObj = entry.getValue();
            if (entryObj instanceof JSONObject){
                title(joiner,(Map<String,Object>)entryObj,(null!=key?key+"_":"")+entryKey);
            }else {
                if (null != entryKey) {
                    String line = (null!=key?key+"_":"") + String.valueOf(entryKey);
                    joiner.add(line);
                }
            }
        }
    }



    public static String CSVLine(Map<String,Object> csvObj,Map<String,Object> filedMap){
        StringJoiner joiner = new StringJoiner(Constants.SPLIT_CHAR);
        join(joiner,csvObj,filedMap);
        return joiner.toString();
    }
    public static void join(StringJoiner joiner,Map<String,Object> csvObj,Map<String,Object> filedMap){
        for (final Map.Entry<String,Object> entry : filedMap.entrySet()){
            String entryKey = entry.getKey();
            Object value = csvObj.getOrDefault(entryKey, Constants.NULL_VALUE);
            if (value instanceof JSONObject){
                join(joiner,(Map<String,Object>)value,(Map<String,Object>)filedMap.get(entryKey));
            }else {
                if (null == value) {
                    value = Constants.NULL_VALUE;
                }
                String line = String.valueOf(value);
                if ("Description".equals(entryKey) || line.contains(Constants.LINE_DELIMITER_DEFAULT) || line.contains(Constants.LINE_DELIMITER_DEFAULT_2)) {
                    line = Constants.ESCAPE_CHARACTER_DOUBLE_QUOTATION + value + Constants.ESCAPE_CHARACTER_DOUBLE_QUOTATION;
                }
                joiner.add(line);
            }
        }
    }
}
