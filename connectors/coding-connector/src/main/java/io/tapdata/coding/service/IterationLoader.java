package io.tapdata.coding.service;

import cn.hutool.json.JSONUtil;
import io.tapdata.coding.CodingHttp;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.Entry;
import io.tapdata.pdk.apis.context.TapConnectionContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static io.tapdata.entity.simplify.TapSimplify.map;

public class IterationLoader extends CodingStarter {
    IterationLoader(TapConnectionContext tapConnectionContext) {
        super(tapConnectionContext);
    }


    public List<Object> loadIteration(List<String> filterTable, Consumer<List<TapTable>> consumer, int tableSize){
        if (null == consumer) {
            throw new IllegalArgumentException("Consumer cannot be null");
        }
        //queryAllIteration();
        return null;
    }

    private List<Map> queryAllIteration(int tableSize) throws Exception {
        DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
        String projectName = connectionConfig.getString("projectName");
        String token = connectionConfig.getString("token");
        String teamName = connectionConfig.getString("teamName");

        int currentQueryCount = 0,queryIndex = 0;

        List<Map> matterList = new ArrayList<>();
        do{
            Map<String,Object> resultMap = CodingHttp.create(
                    map(new Entry("Authorization", token)),
                    map(
                            entry("Action","DescribeIterationList"),
                            entry("ProjectName",projectName),
//                            entry("Status", IssueType.ALL.getName()),
//                            entry("Assignee",""),
//                            entry("StartDate",""),
//                            entry("EndDate",""),
//                            entry("keywords",""),
                            entry("Limit", tableSize),
                            entry("Offset",++queryIndex)
                    ),
                    String.format(OPEN_API_URL,teamName)
            ).post();
            Object response = resultMap.get("Response");
            Map<String,Object> responseMap = null !=  response? JSONUtil.parseObj(response) : null;
            if (null == response){
                if (queryIndex > 1) {
                    queryIndex -= 1;
                    break;
                }else {
                    throw new Exception("discover error");
                }
            }

            currentQueryCount = Integer.parseInt(String.valueOf(resultMap.get("PageSize")));
            Map<String,Object> dataMap = null != responseMap.get("data") ? JSONUtil.parseObj(responseMap.get("data")) : null;
            if (null == dataMap || null == dataMap.get("List")){
                break;
            }
            List<Map> list = JSONUtil.toList(JSONUtil.parseArray(dataMap.get("List")),Map.class);


            matterList.addAll(list);
            //if (null == responseMap || null == responseMap.get("Project")){
            //    throw new Exception("Incorrect project name entered!");
            //}
        }while (currentQueryCount<tableSize);

        return matterList;
    }

}
