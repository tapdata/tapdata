package io.tapdata.coding.service.loader;

import cn.hutool.json.*;
import io.tapdata.coding.service.schema.EnabledSchemas;
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
            String token = connectionConfig.getString("token");
            CodingHttp.create(
                    HttpEntity.create().builder("Authorization",this.tokenSetter(token)).getEntity(),
                    HttpEntity.create().builder("Action","DescribeCodingCurrentUser").getEntity(),
                    String.format(OPEN_API_URL, connectionConfig.get("teamName"))
            ).post();
            return testItem(CodingTestItem.CONNECTION_TEST.getContent(),TestItem.RESULT_SUCCESSFULLY);
        }catch (Exception e){
            return testItem(CodingTestItem.CONNECTION_TEST.getContent(),TestItem.RESULT_FAILED,e.getMessage());
        }
    }

    //测试token
    public TestItem testToken(){
      try {
//          DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
//          HashMap<String, String> headers = new HashMap<>();//存放请求头，可以存放多个请求头
//
//          String token = connectionConfig.getString("token");
//          token = tokenSetter(token);
//          headers.put("Authorization", token);
//          connectionConfig.put("token",token);
//          Map<String,Object> resultMap = CodingHttp.create(
//                  headers,
//                  HttpEntity.create().builderIfNotAbsent("Action","DescribeTeamMembers").builder("PageNumber",1).builder("PageSize",1).getEntity(),
//                  String.format(OPEN_API_URL,connectionConfig.get("teamName"))
//          ).post();
//
//          if (null==resultMap || null==resultMap.get("Response")){
//              throw new Exception("Incorrect token entered!");
//          }
          try {
              EnabledSchemas.getAllSchemas(tapConnectionContext,null);
          }catch (Exception e){
              return testItem(CodingTestItem.TOKEN_TEST.getContent(),TestItem.RESULT_SUCCESSFULLY_WITH_WARN,e.getMessage());
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
            String token = connectionConfig.getString("token");
            Map<String,Object> resultMap = CodingHttp.create(
                    HttpEntity.create().builder("Authorization",this.tokenSetter(token)).getEntity(),
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
