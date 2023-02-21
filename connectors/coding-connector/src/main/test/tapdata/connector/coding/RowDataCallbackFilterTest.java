package tapdata.connector.coding;

import com.alibaba.fastjson.JSON;
import io.tapdata.coding.CodingConnector;
import io.tapdata.coding.entity.ContextConfig;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;

import java.util.Map;

import static io.tapdata.base.ConnectorBase.entry;
import static io.tapdata.base.ConnectorBase.map;

public class RowDataCallbackFilterTest {
    public static RowDataCallbackFilterTest create(){
        return new RowDataCallbackFilterTest();
    }
   public void searchIteration(){
       DataMap config = new DataMap();
       config.put("token","token 0190b04d98dec1cdd7a2825388c17a81fdebd08f");//token 68042a4bad082da78dc44118b4d3e3ec4bd44c6d
       config.put("teamName","testhookgavin");//tapdata
       config.put("projectName","TestIssue");//DFS
       TapConnectionContext context = new TapConnectionContext(null,config,DataMap.create());
       String command = "DescribeIterationList";//"DescribeIterationList";
       String action = "search";
       Map<String, Object> argMap = map(
               entry("page",1),
               entry("size",10)
               ,entry("key","Test")
       );
       CodingConnector codingConnector = new CodingConnector();
       //codingConnector.test(context, command, action, argMap);
   }
   public void listIteration(){
       DataMap config = new DataMap();
       config.put("token","token 0190b04d98dec1cdd7a2825388c17a81fdebd08f");//token 68042a4bad082da78dc44118b4d3e3ec4bd44c6d
       config.put("teamName","testhookgavin");//tapdata
       config.put("projectName","TestIssue");//DFS
       TapConnectionContext context = new TapConnectionContext(null,config,DataMap.create());
       String command = "DescribeIterationList";//"DescribeIterationList";
       String action = "list";
       Map<String, Object> argMap = map(
               entry("page",1),
               entry("size",10)
       );
       CodingConnector codingConnector = new CodingConnector();
       //codingConnector.test(context, command, action, argMap);
   }
   public void searchProject(){
       DataMap config = new DataMap();
       config.put("token","token 0190b04d98dec1cdd7a2825388c17a81fdebd08f");//token 68042a4bad082da78dc44118b4d3e3ec4bd44c6d
       config.put("teamName","testhookgavin");//tapdata
       config.put("projectName","TestIssue");//DFS
       TapConnectionContext context = new TapConnectionContext(null,config,DataMap.create());
       String command = "DescribeCodingProjects";//"DescribeIterationList";
       String action = "search";
       Map<String, Object> argMap = map(
               entry("page",1),
               entry("size",10)
               ,entry("key","TestIssue")
       );
       CodingConnector codingConnector = new CodingConnector();
       //codingConnector.test(context, command, action, argMap);
   }
   public void listProject(){
       DataMap config = new DataMap();
       config.put("token","token 0190b04d98dec1cdd7a2825388c17a81fdebd08f");//token 68042a4bad082da78dc44118b4d3e3ec4bd44c6d
       config.put("teamName","testhookgavin");//tapdata
       config.put("projectName","TestIssue");//DFS
       TapConnectionContext context = new TapConnectionContext(null,config,DataMap.create());
       String command = "DescribeCodingProjects";//"DescribeIterationList";
       String action = "list";
       Map<String, Object> argMap = map(
               entry("page",1),
               entry("size",10)
       );
       CodingConnector codingConnector = new CodingConnector();
       //codingConnector.test(context, command, action, argMap);
   }

   public void exportCSV(){
       DataMap config = new DataMap();
       config.put("token","token 0190b04d98dec1cdd7a2825388c17a81fdebd08f");//token 68042a4bad082da78dc44118b4d3e3ec4bd44c6d
       config.put("teamName","testhookgavin");//tapdata
       config.put("projectName","TestIssue");//DFS
       TapConnectionContext context = new TapConnectionContext(null,config,DataMap.create());
       ContextConfig configs = ContextConfig.create()
               .issueType("ALL").projectName("TestIssue").iterationCodes("1").teamName("testhookgavin").token("token 0190b04d98dec1cdd7a2825388c17a81fdebd08f");

//       RecordStream  stream =new RecordStream(Constants.CACHE_BUFFER_SIZE, Constants.CACHE_BUFFER_COUNT);
//       int size = IssueLoader.create(context)
//               .exportCSV(
//                       100,
////                       "D://issue.csv",
//                       MapUtil.fileds(),
//                       configs,
//                       true,
//                       stream,
//                       0
//               );
//       System.out.println(size);
   }
   public void exportCSVOfFile(){
       DataMap config = new DataMap();
       config.put("token","token 0190b04d98dec1cdd7a2825388c17a81fdebd08f");//token 68042a4bad082da78dc44118b4d3e3ec4bd44c6d
       config.put("teamName","testhookgavin");//tapdata
       config.put("projectName","TestIssue");//DFS
       TapConnectionContext context = new TapConnectionContext(null,config,DataMap.create());
       ContextConfig configs = ContextConfig.create()
               .issueType("ALL").projectName("TestIssue").iterationCodes("1").teamName("testhookgavin").token("token 0190b04d98dec1cdd7a2825388c17a81fdebd08f");

//       IssueLoader.create(context)
//               .exportCSV(
//                       100,
//                       "D://issue.csv",
//                       MapUtil.fileds(),
//                       configs
//               );
   }

    public static void main(String[] args) {
//        RowDataCallbackFilterTest.create().searchIteration();
//        RowDataCallbackFilterTest.create().listIteration();
//        RowDataCallbackFilterTest.create().searchProject();
//        RowDataCallbackFilterTest.create().listProject();
//        RowDataCallbackFilterTest.create().exportCSV();
//        RowDataCallbackFilterTest.create().exportCSVOfFile();

        System.out.println(JSON.toJSON(JSON.parse("{\"text\":\" Hello! This is lark !\"}")));
    }

}
