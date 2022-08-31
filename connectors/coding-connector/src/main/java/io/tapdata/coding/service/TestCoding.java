package io.tapdata.coding.service;

import cn.hutool.json.*;
import io.tapdata.coding.utils.http.CodingHttp;
import io.tapdata.coding.enums.CodingTestItem;
import io.tapdata.coding.utils.http.HttpEntity;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.TestItem;

import java.util.HashMap;
import java.util.Map;

import static io.tapdata.base.ConnectorBase.testItem;

/**
 * @author Gavin
 * @Description CodingTestItem
 * @create 2022-08-24 10:16
 * token 68042a4bad082da78dc44118b4d3e3ec4bd44c6d   DFS         tapdata.net
 *
 * token 0190b04d98dec1cdd7a2825388c17a81fdebd08f   TestIssue   testhookgavin.net
 **/
public class TestCoding extends CodingStarter{

    public static TestCoding create(TapConnectionContext tapConnectionContext){
        return new TestCoding(tapConnectionContext);
    }

    public TestCoding(TapConnectionContext tapConnectionContext){
        super(tapConnectionContext);
    }

    //测试团队名称
    public TestItem testItemConnection(){
        try {
            DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
            CodingHttp.create(
                    HttpEntity.create().builder("Authorization",connectionConfig.getString("token")).getEntity(),
                    null,
                    String.format(CONNECTION_URL, connectionConfig.get("teamName"))
            ).post();
            return testItem(CodingTestItem.CONNECTION_TEST.getContent(),TestItem.RESULT_SUCCESSFULLY);
        }catch (Exception e){
            return testItem(CodingTestItem.CONNECTION_TEST.getContent(),TestItem.RESULT_FAILED,e.getMessage());
        }
    }

    //测试token
    public TestItem testToken(){
      try {
          DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
          HashMap<String, String> headers = new HashMap<>();//存放请求头，可以存放多个请求头

          String token = connectionConfig.getString("token");
          if (!token.startsWith("Token ")&&!token.startsWith("token ")){
              token = "token " + token;
          }
          token = "t" + token.substring(1);
          headers.put("Authorization", token);
          connectionConfig.put("token",token);
          Map<String,Object> resultMap = CodingHttp.create(
                  HttpEntity.create().builder("Authorization",connectionConfig.getString("token")).getEntity(),
                  null,
                  String.format(TOKEN_URL,connectionConfig.get("teamName"))
          ).post();

          if (null==resultMap || null==resultMap.get("id")){
              throw new Exception("Incorrect token entered!");
          }
          return testItem(CodingTestItem.TOKEN_TEST.getContent(),TestItem.RESULT_SUCCESSFULLY);
      }catch (Exception e){
          return testItem(CodingTestItem.TOKEN_TEST.getContent(),TestItem.RESULT_FAILED,e.getMessage());
      }
    }

    //测试项目
    public TestItem testProject(){
        try {
            DataMap connectionConfig = tapConnectionContext.getConnectionConfig();

            Map<String,Object> resultMap = CodingHttp.create(
                    HttpEntity.create().builder("Authorization",connectionConfig.getString("token")).getEntity(),
                    HttpEntity.create()
                            .builder("Action","DescribeProjectByName")
                            .builder("ProjectName",connectionConfig.get("projectName"))
                            .getEntity(),
                    String.format(OPEN_API_URL,connectionConfig.get("teamName"))
            ).post();

            Map<String,Object> responseMap =
                    null != resultMap.get("Response") ? JSONUtil.parseObj(resultMap.get("Response")) : null;
            if (null == responseMap || null == responseMap.get("Project")){
                throw new Exception("Incorrect project name entered!");
            }
            return testItem(CodingTestItem.PROJECT_TEST.getContent(),TestItem.RESULT_SUCCESSFULLY);
        }catch (Exception e){
            return testItem(CodingTestItem.PROJECT_TEST.getContent(),TestItem.RESULT_FAILED,e.getMessage());
        }
    }

}
