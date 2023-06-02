package tapdata.connector;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static io.tapdata.base.ConnectorBase.fromJson;

/**
 * @author GavinXiao
 * @description QPSTest create by Gavin
 * @create 2023/5/31 17:38
 **/
public class QPSTest {
    @Test
    public void testQps(){
        while (true) {
            invoke((Map<String, Object>) fromJson("{\n" +
                    "    \"action\": \"update_assignee\",\n" +
                    "    \"event\": \"ISSUE_ASSIGNEE_CHANGED\",\n" +
                    "    \"eventName\": \"分配处理人\",\n" +
                    "    \"sender\": {\n" +
                    "        \"id\": 8390151,\n" +
                    "        \"login\": \"oxDejUAvpG\",\n" +
                    "        \"avatar_url\": \"https://coding-net-production-static-ci.codehub.cn/daef2f8d-1e6b-404e-a83e-8100970e14ba.jpg?imageView2/1/w/0/h/0\",\n" +
                    "        \"url\": \"https://tapdata.coding.net/api/user/key/oxDejUAvpG\",\n" +
                    "        \"html_url\": \"https://tapdata.coding.net/u/oxDejUAvpG\",\n" +
                    "        \"name\": \"Erin\",\n" +
                    "        \"name_pinyin\": \"Erin\"\n" +
                    "    },\n" +
                    "    \"project\": {\n" +
                    "        \"id\": 342870,\n" +
                    "        \"icon\": \"https://dn-coding-net-production-pp.codehub.cn/79a8bcc4-d9cc-4061-940d-5b3bb31bf571.png\",\n" +
                    "        \"url\": \"https://tapdata.coding.net/p/tapdata\",\n" +
                    "        \"description\": \"Tapdata DaaS \",\n" +
                    "        \"name\": \"tapdata\",\n" +
                    "        \"display_name\": \"Tapdata DaaS\"\n" +
                    "    },\n" +
                    "    \"team\": {\n" +
                    "        \"id\": 155077,\n" +
                    "        \"domain\": \"tapdata\",\n" +
                    "        \"url\": \"https://tapdata.coding.net\",\n" +
                    "        \"introduction\": \"\",\n" +
                    "        \"name\": \"Tapdata\",\n" +
                    "        \"name_pinyin\": \"Tapdata\",\n" +
                    "        \"avatar\": \"https://coding-net-production-pp-ci.codehub.cn/9837b4a6-442b-4513-b51d-a2030c4a6ede.png\"\n" +
                    "    },\n" +
                    "    \"defect\": {\n" +
                    "        \"html_url\": \"https://tapdata.coding.net/p/tapdata/bug-tracking/issues/133415/detail\",\n" +
                    "        \"typeZh\": \"缺陷\",\n" +
                    "        \"statusZh\": \"进行中\",\n" +
                    "        \"type\": \"DEFECT\",\n" +
                    "        \"project_id\": 342870,\n" +
                    "        \"code\": 133415,\n" +
                    "        \"parent_code\": 0,\n" +
                    "        \"title\": \"#0000: Cloud Auto Issue: 63d5e5be90c8e908c673bc5b\",\n" +
                    "        \"creator\": {\n" +
                    "            \"id\": 8054404,\n" +
                    "            \"login\": \"fDfggXszTq\",\n" +
                    "            \"avatar_url\": \"https://coding-net-production-static-ci.codehub.cn/WM-TEXT-AVATAR-eIKPrrFIbZvWEBGUurtc.jpg\",\n" +
                    "            \"url\": \"https://tapdata.coding.net/api/user/key/fDfggXszTq\",\n" +
                    "            \"html_url\": \"https://tapdata.coding.net/u/fDfggXszTq\",\n" +
                    "            \"name\": \"Berry\",\n" +
                    "            \"name_pinyin\": \"Berry\",\n" +
                    "            \"email\": \"\",\n" +
                    "            \"phone\": \"\"\n" +
                    "        },\n" +
                    "        \"status\": \"已拒绝\",\n" +
                    "        \"assignee\": {\n" +
                    "            \"id\": 222013,\n" +
                    "            \"login\": \"KjsNDwGkjV\",\n" +
                    "            \"avatar_url\": \"https://coding-net-production-static-ci.codehub.cn/WM-TEXT-AVATAR-JcomTlKlmEDqkrhrbHwc.jpg\",\n" +
                    "            \"url\": \"https://tapdata.coding.net/api/user/key/KjsNDwGkjV\",\n" +
                    "            \"html_url\": \"https://tapdata.coding.net/u/KjsNDwGkjV\",\n" +
                    "            \"name\": \"TJ\",\n" +
                    "            \"name_pinyin\": \"TJ\",\n" +
                    "            \"email\": \"\",\n" +
                    "            \"phone\": \"\"\n" +
                    "        },\n" +
                    "        \"priority\": 2,\n" +
                    "        \"due_date\": 1676044799000,\n" +
                    "        \"iteration\": {\n" +
                    "            \"title\": \"sprint #63\",\n" +
                    "            \"goal\": \"\",\n" +
                    "            \"html_url\": \"https://tapdata.coding.net/p/tapdata/iterations/126723\",\n" +
                    "            \"project_id\": 342870,\n" +
                    "            \"code\": 126723,\n" +
                    "            \"creator\": {\n" +
                    "                \"id\": 8136202,\n" +
                    "                \"login\": \"EKPSyphfdZ\",\n" +
                    "                \"avatar_url\": \"https://coding-net-production-static-ci.codehub.cn/WM-TEXT-AVATAR-AUxzbbMIyLfUtPQGZspa.jpg\",\n" +
                    "                \"url\": \"https://tapdata.coding.net/api/user/key/EKPSyphfdZ\",\n" +
                    "                \"html_url\": \"https://tapdata.coding.net/u/EKPSyphfdZ\",\n" +
                    "                \"name\": \"Martin\",\n" +
                    "                \"name_pinyin\": \"Martin\",\n" +
                    "                \"email\": \"\",\n" +
                    "                \"phone\": \"\"\n" +
                    "            },\n" +
                    "            \"assignee\": {\n" +
                    "                \"id\": 8390239,\n" +
                    "                \"login\": \"YxUqazDdZu\",\n" +
                    "                \"avatar_url\": \"https://coding-net-production-static-ci.codehub.cn/b42fac81-a996-44f4-a89c-7eed5035378b.jpg?imageView2/1/w/0/h/0\",\n" +
                    "                \"url\": \"https://tapdata.coding.net/api/user/key/YxUqazDdZu\",\n" +
                    "                \"html_url\": \"https://tapdata.coding.net/u/YxUqazDdZu\",\n" +
                    "                \"name\": \"Philbert\",\n" +
                    "                \"name_pinyin\": \"Philbert\",\n" +
                    "                \"email\": \"\",\n" +
                    "                \"phone\": \"\"\n" +
                    "            },\n" +
                    "            \"watchers\": [\n" +
                    "                {\n" +
                    "                    \"id\": 8390239,\n" +
                    "                    \"login\": \"YxUqazDdZu\",\n" +
                    "                    \"avatar_url\": \"https://coding-net-production-static-ci.codehub.cn/b42fac81-a996-44f4-a89c-7eed5035378b.jpg?imageView2/1/w/0/h/0\",\n" +
                    "                    \"url\": \"\",\n" +
                    "                    \"html_url\": \"\",\n" +
                    "                    \"name\": \"Philbert\",\n" +
                    "                    \"name_pinyin\": \"Philbert\",\n" +
                    "                    \"email\": \"\",\n" +
                    "                    \"phone\": \"\"\n" +
                    "                }\n" +
                    "            ],\n" +
                    "            \"status\": \"PROCESSING\",\n" +
                    "            \"plan_issue_number\": 257,\n" +
                    "            \"start_at\": 1672502400000,\n" +
                    "            \"end_at\": 1675180799000,\n" +
                    "            \"created_at\": 1667633140000,\n" +
                    "            \"updated_at\": 1682236954000\n" +
                    "        },\n" +
                    "        \"description\": \"\\n任务 ID: 63d5e5be90c8e908c673bc5b \\n\\n运行 ID: 63d5e90c90c8e908c6741103 \\n\\n任务名: 新任务@上午11:19:27 \\n\\n简要信息: Starting stream read failed, errors: start point offset is null \\n\\n创建人: wechat_efog58 \\n\\n报错时间: 1674963223248 \\n\\n错误堆栈: Starting stream read failed, errors: start point offset is null\\n io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.HazelcastSourcePdkDataNode.doCdc(HazelcastSourcePdkDataNode.java:390)\\n io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.HazelcastSourcePdkDataNode.startSourceRunner(HazelcastSourcePdkDataNode.java:119)\\n java.util.concurrent.Executors$RunnableAdapter.call(Unknown Source)\\n java.util.concurrent.FutureTask.run(Unknown Source)\\n java.util.concurrent.ThreadPoolExecutor.runWorker(Unknown Source)\\n java.util.concurrent.ThreadPoolExecutor$Worker.run(Unknown Source)\\n java.lang.Thread.run(Unknown Source) \\n\",\n" +
                    "        \"created_at\": 1674973839000,\n" +
                    "        \"updated_at\": 1675224192000,\n" +
                    "        \"issue_status\": {\n" +
                    "            \"id\": 1587669,\n" +
                    "            \"name\": \"已拒绝\",\n" +
                    "            \"type\": \"PROCESSING\"\n" +
                    "        },\n" +
                    "        \"watchers\": [\n" +
                    "            {\n" +
                    "                \"id\": 222013,\n" +
                    "                \"login\": \"KjsNDwGkjV\",\n" +
                    "                \"avatar_url\": \"https://coding-net-production-static-ci.codehub.cn/WM-TEXT-AVATAR-JcomTlKlmEDqkrhrbHwc.jpg\",\n" +
                    "                \"url\": \"\",\n" +
                    "                \"html_url\": \"\",\n" +
                    "                \"name\": \"TJ\",\n" +
                    "                \"name_pinyin\": \"TJ\",\n" +
                    "                \"email\": \"\",\n" +
                    "                \"phone\": \"\"\n" +
                    "            }\n" +
                    "        ],\n" +
                    "        \"labels\": []\n" +
                    "    },\n" +
                    "    \"issue\": {\n" +
                    "        \"html_url\": \"https://tapdata.coding.net/p/tapdata/bug-tracking/issues/133415/detail\",\n" +
                    "        \"typeZh\": \"缺陷\",\n" +
                    "        \"statusZh\": \"进行中\",\n" +
                    "        \"type\": \"DEFECT\",\n" +
                    "        \"project_id\": 342870,\n" +
                    "        \"code\": 133415,\n" +
                    "        \"parent_code\": 0,\n" +
                    "        \"title\": \"#0000: Cloud Auto Issue: 63d5e5be90c8e908c673bc5b\",\n" +
                    "        \"creator\": {\n" +
                    "            \"id\": 8054404,\n" +
                    "            \"login\": \"fDfggXszTq\",\n" +
                    "            \"avatar_url\": \"https://coding-net-production-static-ci.codehub.cn/WM-TEXT-AVATAR-eIKPrrFIbZvWEBGUurtc.jpg\",\n" +
                    "            \"url\": \"https://tapdata.coding.net/api/user/key/fDfggXszTq\",\n" +
                    "            \"html_url\": \"https://tapdata.coding.net/u/fDfggXszTq\",\n" +
                    "            \"name\": \"Berry\",\n" +
                    "            \"name_pinyin\": \"Berry\",\n" +
                    "            \"email\": \"\",\n" +
                    "            \"phone\": \"\"\n" +
                    "        },\n" +
                    "        \"status\": \"已拒绝\",\n" +
                    "        \"assignee\": {\n" +
                    "            \"id\": 222013,\n" +
                    "            \"login\": \"KjsNDwGkjV\",\n" +
                    "            \"avatar_url\": \"https://coding-net-production-static-ci.codehub.cn/WM-TEXT-AVATAR-JcomTlKlmEDqkrhrbHwc.jpg\",\n" +
                    "            \"url\": \"https://tapdata.coding.net/api/user/key/KjsNDwGkjV\",\n" +
                    "            \"html_url\": \"https://tapdata.coding.net/u/KjsNDwGkjV\",\n" +
                    "            \"name\": \"TJ\",\n" +
                    "            \"name_pinyin\": \"TJ\",\n" +
                    "            \"email\": \"\",\n" +
                    "            \"phone\": \"\"\n" +
                    "        },\n" +
                    "        \"priority\": 2,\n" +
                    "        \"due_date\": 1676044799000,\n" +
                    "        \"iteration\": {\n" +
                    "            \"title\": \"sprint #63\",\n" +
                    "            \"goal\": \"\",\n" +
                    "            \"html_url\": \"https://tapdata.coding.net/p/tapdata/iterations/126723\",\n" +
                    "            \"project_id\": 342870,\n" +
                    "            \"code\": 126723,\n" +
                    "            \"creator\": {\n" +
                    "                \"id\": 8136202,\n" +
                    "                \"login\": \"EKPSyphfdZ\",\n" +
                    "                \"avatar_url\": \"https://coding-net-production-static-ci.codehub.cn/WM-TEXT-AVATAR-AUxzbbMIyLfUtPQGZspa.jpg\",\n" +
                    "                \"url\": \"https://tapdata.coding.net/api/user/key/EKPSyphfdZ\",\n" +
                    "                \"html_url\": \"https://tapdata.coding.net/u/EKPSyphfdZ\",\n" +
                    "                \"name\": \"Martin\",\n" +
                    "                \"name_pinyin\": \"Martin\",\n" +
                    "                \"email\": \"\",\n" +
                    "                \"phone\": \"\"\n" +
                    "            },\n" +
                    "            \"assignee\": {\n" +
                    "                \"id\": 8390239,\n" +
                    "                \"login\": \"YxUqazDdZu\",\n" +
                    "                \"avatar_url\": \"https://coding-net-production-static-ci.codehub.cn/b42fac81-a996-44f4-a89c-7eed5035378b.jpg?imageView2/1/w/0/h/0\",\n" +
                    "                \"url\": \"https://tapdata.coding.net/api/user/key/YxUqazDdZu\",\n" +
                    "                \"html_url\": \"https://tapdata.coding.net/u/YxUqazDdZu\",\n" +
                    "                \"name\": \"Philbert\",\n" +
                    "                \"name_pinyin\": \"Philbert\",\n" +
                    "                \"email\": \"\",\n" +
                    "                \"phone\": \"\"\n" +
                    "            },\n" +
                    "            \"watchers\": [\n" +
                    "                {\n" +
                    "                    \"id\": 8390239,\n" +
                    "                    \"login\": \"YxUqazDdZu\",\n" +
                    "                    \"avatar_url\": \"https://coding-net-production-static-ci.codehub.cn/b42fac81-a996-44f4-a89c-7eed5035378b.jpg?imageView2/1/w/0/h/0\",\n" +
                    "                    \"url\": \"\",\n" +
                    "                    \"html_url\": \"\",\n" +
                    "                    \"name\": \"Philbert\",\n" +
                    "                    \"name_pinyin\": \"Philbert\",\n" +
                    "                    \"email\": \"\",\n" +
                    "                    \"phone\": \"\"\n" +
                    "                }\n" +
                    "            ],\n" +
                    "            \"status\": \"PROCESSING\",\n" +
                    "            \"plan_issue_number\": 257,\n" +
                    "            \"start_at\": 1672502400000,\n" +
                    "            \"end_at\": 1675180799000,\n" +
                    "            \"created_at\": 1667633140000,\n" +
                    "            \"updated_at\": 1682236954000\n" +
                    "        },\n" +
                    "        \"description\": \"\\n任务 ID: 63d5e5be90c8e908c673bc5b \\n\\n运行 ID: 63d5e90c90c8e908c6741103 \\n\\n任务名: 新任务@上午11:19:27 \\n\\n简要信息: Starting stream read failed, errors: start point offset is null \\n\\n创建人: wechat_efog58 \\n\\n报错时间: 1674963223248 \\n\\n错误堆栈: Starting stream read failed, errors: start point offset is null\\n io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.HazelcastSourcePdkDataNode.doCdc(HazelcastSourcePdkDataNode.java:390)\\n io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.HazelcastSourcePdkDataNode.startSourceRunner(HazelcastSourcePdkDataNode.java:119)\\n java.util.concurrent.Executors$RunnableAdapter.call(Unknown Source)\\n java.util.concurrent.FutureTask.run(Unknown Source)\\n java.util.concurrent.ThreadPoolExecutor.runWorker(Unknown Source)\\n java.util.concurrent.ThreadPoolExecutor$Worker.run(Unknown Source)\\n java.lang.Thread.run(Unknown Source) \\n\",\n" +
                    "        \"created_at\": 1674973839000,\n" +
                    "        \"updated_at\": 1675224192000,\n" +
                    "        \"issue_status\": {\n" +
                    "            \"id\": 1587669,\n" +
                    "            \"name\": \"已拒绝\",\n" +
                    "            \"type\": \"PROCESSING\"\n" +
                    "        },\n" +
                    "        \"watchers\": [\n" +
                    "            {\n" +
                    "                \"id\": 222013,\n" +
                    "                \"login\": \"KjsNDwGkjV\",\n" +
                    "                \"avatar_url\": \"https://coding-net-production-static-ci.codehub.cn/WM-TEXT-AVATAR-JcomTlKlmEDqkrhrbHwc.jpg\",\n" +
                    "                \"url\": \"\",\n" +
                    "                \"html_url\": \"\",\n" +
                    "                \"name\": \"TJ\",\n" +
                    "                \"name_pinyin\": \"TJ\",\n" +
                    "                \"email\": \"\",\n" +
                    "                \"phone\": \"\"\n" +
                    "            }\n" +
                    "        ],\n" +
                    "        \"labels\": []\n" +
                    "    },\n" +
                    "    \"hook\": {\n" +
                    "        \"id\": \"e61e0165-86a3-4223-9241-5dc6bc8c43a0\",\n" +
                    "        \"name\": \"web\",\n" +
                    "        \"type\": \"Repository\",\n" +
                    "        \"active\": false,\n" +
                    "        \"events\": [\n" +
                    "            \"ISSUE_HOUR_RECORD_UPDATED\",\n" +
                    "            \"ISSUE_COMMENT_CREATED\",\n" +
                    "            \"ISSUE_RELATIONSHIP_CHANGED\",\n" +
                    "            \"ISSUE_DELETED\",\n" +
                    "            \"ISSUE_ITERATION_CHANGED\",\n" +
                    "            \"ISSUE_ASSIGNEE_CHANGED\",\n" +
                    "            \"ISSUE_STATUS_UPDATED\",\n" +
                    "            \"ISSUE_CREATED\",\n" +
                    "            \"ISSUE_UPDATED\"\n" +
                    "        ],\n" +
                    "        \"config\": {\n" +
                    "            \"content_type\": \"application/json\",\n" +
                    "            \"url\": \"http://139.198.127.204:30564/api/proxy/callback/zIh1SiyQScDto-dc3maRzOWfHKufHQBQD9GyzIpTAUZv_7cmSYEVwB3sSlEVsq-iRw==\"\n" +
                    "        },\n" +
                    "        \"created_at\": 1684402135000,\n" +
                    "        \"updated_at\": 1685500249000\n" +
                    "    },\n" +
                    "    \"hook_id\": \"e61e0165-86a3-4223-9241-5dc6bc8c43a0\"\n" +
                    "}"));
        }
    }

    public Object invoke(Map<String, Object> params) {
        try {
            String apiUrl = "http://localhost:8080/api/proxy/callback/zIh1SiyQScDto-dd3DGQm7WcHKueH1ZRDYe-zY9SBRJo_7cmSYEVwB3sSlEVqLFB2A==";

            URL url = new URL(apiUrl);
            // 组织请求参数
            StringBuilder postBody = new StringBuilder();
            for (Map.Entry<String, Object> entry : params.entrySet()) {
                if (entry.getValue() == null) continue;
                postBody.append(entry.getKey()).append("=").append(URLEncoder.encode(entry.getValue().toString(),"utf-8")).append("&");
            }

            if (!params.isEmpty()) {
                postBody.deleteCharAt(postBody.length() - 1);
            }
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestProperty("Connection", "Keep-Alive");
            conn.setConnectTimeout(1000);
            conn.setReadTimeout(3000);
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.getOutputStream().write(postBody.toString().getBytes(StandardCharsets.UTF_8));
            conn.getOutputStream().flush();
            int responseCode = conn.getResponseCode();
            if (responseCode != 200) {
                //logger.warn("invoke failed, response status:" + responseCode);
                return null;
            }

            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder();
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                result.append(line).append("\n");
            }
            return result.toString().trim();
        } catch (Exception e) {
            //logger.error("invoke throw exception, details: " + e);
        }
        return null;
    }
}
