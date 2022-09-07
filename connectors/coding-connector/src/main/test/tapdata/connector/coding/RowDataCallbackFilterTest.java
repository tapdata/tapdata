package tapdata.connector.coding;

import io.tapdata.coding.CodingConnector;
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
       config.put("token","token 68042a4bad082da78dc44118b4d3e3ec4bd44c6d");//token 0190b04d98dec1cdd7a2825388c17a81fdebd08f
       config.put("teamName","tapdata");//testhookgavin
       config.put("projectName","DFS");//TestIssue
       TapConnectionContext context = new TapConnectionContext(null,config);
       String command = "DescribeCodingProjects";//"DescribeIterationList";
       String action = "search";
       Map<String, Object> argMap = map(
               entry("page",1),
               entry("size",10)
               ,entry("key","DF")
       );
       CodingConnector codingConnector = new CodingConnector();
       codingConnector.test(context, command, action, argMap);
   }
   public void listIteration(){
       DataMap config = new DataMap();
       config.put("token","token 68042a4bad082da78dc44118b4d3e3ec4bd44c6d");//token 0190b04d98dec1cdd7a2825388c17a81fdebd08f
       config.put("teamName","tapdata");//testhookgavin
       config.put("projectName","DFS");//TestIssue
       TapConnectionContext context = new TapConnectionContext(null,config);
       String command = "DescribeCodingProjects";//"DescribeIterationList";
       String action = "list";
       Map<String, Object> argMap = map(
               entry("page",1),
               entry("size",10)
       );
       CodingConnector codingConnector = new CodingConnector();
       codingConnector.test(context, command, action, argMap);
   }
   public void searchProject(){
       DataMap config = new DataMap();
       config.put("token","token 0190b04d98dec1cdd7a2825388c17a81fdebd08f");//token 68042a4bad082da78dc44118b4d3e3ec4bd44c6d
       config.put("teamName","testhookgavin");//tapdata
       config.put("projectName","TestIssue");//DFS
       TapConnectionContext context = new TapConnectionContext(null,config);
       String command = "DescribeCodingProjects";//"DescribeIterationList";
       String action = "search";
       Map<String, Object> argMap = map(
               entry("page",1),
               entry("size",10)
               ,entry("key","TestIssue")
       );
       CodingConnector codingConnector = new CodingConnector();
       codingConnector.test(context, command, action, argMap);
   }
   public void listProject(){
       DataMap config = new DataMap();
       config.put("token","token 0190b04d98dec1cdd7a2825388c17a81fdebd08f");//token 68042a4bad082da78dc44118b4d3e3ec4bd44c6d
       config.put("teamName","testhookgavin");//tapdata
       config.put("projectName","TestIssue");//DFS
       TapConnectionContext context = new TapConnectionContext(null,config);
       String command = "DescribeCodingProjects";//"DescribeIterationList";
       String action = "list";
       Map<String, Object> argMap = map(
               entry("page",1),
               entry("size",10)
       );
       CodingConnector codingConnector = new CodingConnector();
       codingConnector.test(context, command, action, argMap);
   }

    public static void main(String[] args) {
        RowDataCallbackFilterTest.create().searchIteration();
//        RowDataCallbackFilterTest.create().listIteration();
//        RowDataCallbackFilterTest.create().searchProject();
//        RowDataCallbackFilterTest.create().listProject();
    }
}
