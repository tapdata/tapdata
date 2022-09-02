package io.tapdata.pdk.core.api.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.functions.ConnectionFunctions;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.core.api.ConnectionNode;
import io.tapdata.pdk.core.api.Node;
import io.tapdata.pdk.core.entity.params.PDKMethodInvoker;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.tapnode.TapNodeInfo;
import io.tapdata.pdk.core.utils.CommonUtils;
import net.sf.cglib.beans.BeanMap;
import org.bson.json.Converter;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RetryTest {
    final String nodeStr = "{" +
            "\"associateId\":\"codingSource_1662100795708\"," +
            "\"connector\":{}," +
            "\"connectionContext\":{" +
            "\"specification\":{" +
            "\"icon\":\"icons/coding.png\"," +
            "\"version\":\"1.0-SNAPSHOT\"," +
            "\"dataTypesMap\":{}," +
            "\"name\":\"Coding\"," +
            "\"id\":\"coding\"," +
            "\"group\":\"io.tapdata\"," +
            "\"configOptions\":{" +
            "\"pdkExpansion\":[]," +
            "\"node\":{" +
            "\"type\":\"object\"," +
            "\"properties\":{" +
            "\"iterationCodes\":{" +
            "\"x-component\":\"Input\"," +
            "\"type\":\"String\"," +
            "\"title\":\"${iterationCodes}\"," +
            "\"x-index\":1," +
            "\"required\":true," +
            "\"x-decorator\":\"FormItem\"" +
            "}," +
            "\"issueType\":{" +
            "\"x-component\":\"Select\"," +
            "\"type\":\"String\"," +
            "\"title\":\"${issueType}\"," +
            "\"x-index\":2," +
            "\"enum\":[" +
            "{\"label\":\"${allIssueType}\",\"value\":\"ALL\"}," +
            "{\"label\":\"${defectIssue}\",\"value\":\"DEFECT\"}," +
            "{\"label\":\"requirementIssue\",\"value\":\"REQUIREMENT\"}," +
            "{\"label\":\"${missionIssue}\",\"value\":\"MISSION\"}," +
            "{\"label\":\"${epicIssue}\",\"value\":\"EPIC\"}" +
            "]," +
            "\"required\":false," +
            "\"default\":\"ALL\"," +
            "\"x-decorator\":\"FormItem\"" +
            "}" +
            "}" +
            "}," +
            "\"connection\":{" +
            "\"type\":\"object\"," +
            "\"properties\":{" +
            "\"teamName\":{" +
            "\"x-component\":\"Input\"," +
            "\"apiServerKey\":\"database_host\"," +
            "\"type\":\"String\"," +
            "\"title\":\"tapdata\"," +
            "\"x-index\":1," +
            "\"required\":true," +
            "\"x-decorator\":\"FormItem\"" +
            "}," +
            "\"token\":{" +
            "\"x-component\":\"Input\"," +
            "\"apiServerKey\":\"database_host\"," +
            "\"type\":\"String\"," +
            "\"title\":\"${token}\"," +
            "\"x-index\":2," +
            "\"required\":true," +
            "\"x-decorator\":\"FormItem\"" +
            "}," +
            "\"projectName\":{" +
            "\"x-component\":\"Input\"," +
            "\"apiServerKey\":\"database_host\"," +
            "\"type\":\"String\"," +
            "\"title\":\"${projectName}\"," +
            "\"x-index\":3," +
            "\"required\":true," +
            "\"x-decorator\":\"FormItem\"" +
            "}" +
            "}" +
            "}" +
            "}" +
            "}," +
            "\"connectionConfig\":{" +
            "\"teamName\":\"tapdata\"," +
            "\"token\":\"token 68042a4bad082da78dc44118b4d3e3ec4bd44c6d\"," +
            "\"projectName\":\"DFS\"" +
            "}," +
            "\"id\":\"4f178fd5ada241149176c5a18344c83e\"" +
            "}," +
            "\"connectionFunctions\":{}," +
            "\"tapNodeInfo\":{" +
            "\"nodeClass\":\"class io.tapdata.coding.CodingConnector\"," +
            "\"tapNodeSpecification\":{" +
            "\"icon\":\"icons/coding.png\"," +
            "\"version\":\"1.0-SNAPSHOT\"," +
            "\"dataTypesMap\":{}," +
            "\"name\":\"Coding\"," +
            "\"id\":\"coding\"," +
            "\"group\":\"io.tapdata\"," +
            "\"configOptions\":{" +
            "\"pdkExpansion\":[]," +
            "\"node\":{" +
            "\"type\":\"object\"," +
            "\"properties\":{" +
            "\"iterationCodes\":{" +
            "\"x-component\":\"Input\"," +
            "\"type\":\"String\"," +
            "\"title\":\"${iterationCodes}\"," +
            "\"x-index\":1," +
            "\"required\":true," +
            "\"x-decorator\":\"FormItem\"" +
            "}," +
            "\"issueType\":{" +
            "\"x-component\":\"Select\"," +
            "\"type\":\"String\"," +
            "\"title\":\"${issueType}\"," +
            "\"x-index\":2," +
            "\"enum\":[" +
            "{\"label\":\"${allIssueType}\",\"value\":\"ALL\"}," +
            "{\"label\":\"${defectIssue}\",\"value\":\"DEFECT\"}," +
            "{\"label\":\"${requirementIssue}\",\"value\":\"REQUIREMENT\"}," +
            "{\"label\":\"${missionIssue}\",\"value\":\"MISSION\"}," +
            "{\"label\":\"${epicIssue}\",\"value\":\"EPIC\"}" +
            "]," +
            "\"required\":false," +
            "\"default\":\"ALL\"," +
            "\"x-decorator\":\"FormItem\"" +
            "}" +
            "}" +
            "}," +
            "\"connection\":{" +
            "\"type\":\"object\"," +
            "\"properties\":{" +
            "\"teamName\":{" +
            "\"x-component\":\"Input\"," +
            "\"apiServerKey\":\"database_host\"," +
            "\"type\":\"String\"," +
            "\"title\":\"${teamName}\"," +
            "\"x-index\":1,\"required\":true," +
            "\"x-decorator\":\"FormItem\"}," +
            "\"token\":{\"x-component\":\"Input\"," +
            "\"apiServerKey\":\"database_host\"," +
            "\"type\":\"String\"," +
            "\"title\":\"${token}\"," +
            "\"x-index\":2," +
            "\"required\":true," +
            "\"x-decorator\":\"FormItem\"" +
            "}," +
            "\"projectName\":{" +
            "\"x-component\":\"Input\"," +
            "\"apiServerKey\":\"database_host\"," +
            "\"type\":\"String\"," +
            "\"title\":\"DFS\"," +
            "\"x-index\":3," +
            "\"required\":true," +
            "\"x-decorator\":\"FormItem\"" +
            "}" +
            "}" +
            "}" +
            "}" +
            "}," +
            "\"nodeType\":\"Source\"" +
            "}" +
            "}";
    //测试报错能否触发重试
    @Test
    public void testRetry() throws Exception {
        Map<String,Object> nodeMap = JSONObject.parseObject(nodeStr);
        ConnectionNode node = new ConnectionNode();


        PDKInvocationMonitor.invoke(
                node,
                PDKMethod.REGISTER_CAPABILITIES,
                PDKMethodInvoker.create()
                        .runnable(()->{System.out.println("Begin retry...");throw new IOException("test retry...");})
                        .message("call connection functions coding@io.tapdata-v1.0-SNAPSHOT associateId codingSource_1662088750311")
                        .logTag("")
                        .errorConsumer(null)
                        .async(false)
                        .contextClassLoader(null)
                        .retryTimes(3)
                        .retryPeriodSeconds(5)
        );
    }
    //测试
    @Test
    public void test2(){

    }

    //测试能否中断重试
    @Test
    public void testAwaken(){
        ConnectionNode node = JSONObject.parseObject(
                "{\"id\":\"coding\",\"group\":\"io.tapdata\",\"version\":\"1.0-SNAPSHOT\",\"nodeType\":\"Source\",\"nodeClass\":\"class io.tapdata.coding.CodingConnector\", \"associateId\":\"codingSource_1662088750311\", \"dagId\":\"null\"}",
                ConnectionNode.class);
        PDKInvocationMonitor.invoke(
                node,
                PDKMethod.REGISTER_CAPABILITIES,
                PDKMethodInvoker.create()
                        .runnable(()->{System.out.println("Begin retry...");throw new IOException("test retry...");})
                        .message("call connection functions coding@io.tapdata-v1.0-SNAPSHOT associateId codingSource_1662088750311")
                        .logTag("")
                        .errorConsumer(null)
                        .async(false)
                        .contextClassLoader(null)
                        .retryTimes(1<<20)
                        .retryPeriodSeconds(1<<20)
        );
        CommonUtils.awakenRetryObj(CommonUtils.AutoRetryParams.class);
//        int a = 10;
//        assertEquals(1,a);
    }
}
