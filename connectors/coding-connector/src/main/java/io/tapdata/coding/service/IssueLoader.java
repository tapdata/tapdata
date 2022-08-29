package io.tapdata.coding.service;

import cn.hutool.json.JSONUtil;
import io.tapdata.coding.CodingHttp;
import io.tapdata.coding.enums.IssueType;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.Entry;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import static io.tapdata.entity.simplify.TapSimplify.map;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;


/**
 * @author GavinX
 * @Description
 * @create 2022-08-26 11:49
 **/
public class IssueLoader extends CodingStarter {
    private static final String TAG = IssueLoader.class.getSimpleName();

    public static IssueLoader create(TapConnectionContext tapConnectionContext) {
        return new IssueLoader(tapConnectionContext);
    }

    int tableSize;

    public IssueLoader(TapConnectionContext tapConnectionContext) {
        super(tapConnectionContext);
    }

    public IssueLoader setTableSize(int tableSize) {
        this.tableSize = tableSize;
        return this;
    }


    public void discoverMatterOldVersion(List<String> filterTable, Consumer<List<TapTable>> consumer) {
        if (null == consumer) {
            throw new IllegalArgumentException("Consumer cannot be null");
        }

    }

    public void discoverIssue(List<String> filterTable, Consumer<List<TapTable>> consumer) throws Exception {
        if (null == consumer) {
            throw new IllegalArgumentException("Consumer cannot be null");
        }

        /** *获取所有事项* */

        List<Map> matterList = this.queryAllIssue();

        /*** 获取每个事项的迭代 */

        ///.....@TODO 加入到TapTable中
    }

    public List<Map> queryAllIssue() throws Exception {
        DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
        String projectName = connectionConfig.getString("projectName");
        String token = connectionConfig.getString("token");
        String teamName = connectionConfig.getString("teamName");

        int currentQueryCount = 0, queryIndex = 0;

        List<Map> matterList = new ArrayList<>();
        do {
            Map<String, Object> resultMap = CodingHttp.create(
                    map(new Entry("Authorization", token)),
                    map(
                            entry("Action", "DescribeIssueListWithPage"),
                            entry("ProjectName", projectName),
                            entry("IssueType", IssueType.ALL.getName()),
                            entry("PageSize", tableSize),
                            entry("PageNumber", ++queryIndex)
                    ),
                    String.format(OPEN_API_URL, teamName)
            ).post();
            Object response = resultMap.get("Response");
            Map<String, Object> responseMap = null != response ? JSONUtil.parseObj(response) : null;
            if (null == response) {
                if (queryIndex > 1) {
                    queryIndex -= 1;
                    break;
                } else {
                    throw new Exception("discover error");
                }
            }

            currentQueryCount = Integer.parseInt(String.valueOf(resultMap.get("PageSize")));
            Map<String, Object> dataMap = null != responseMap.get("Data") ? JSONUtil.parseObj(responseMap.get("Data")) : null;
            if (null == dataMap || null == dataMap.get("List")) {
                break;
            }
            List<Map> list = JSONUtil.toList(JSONUtil.parseArray(dataMap.get("List")), Map.class);


            matterList.addAll(list);
            //if (null == responseMap || null == responseMap.get("Project")){
            //    throw new Exception("Incorrect project name entered!");
            //}
        } while (currentQueryCount < tableSize);

        return matterList;
    }


    public void batchReadIssue(TapTable table,
                               Object offset,
                               int batchCount,
                               BiConsumer<List<TapEvent>, Object> consumer) {
        List<TapEvent> tempList = new ArrayList<>();
        consumer.accept(null, null);
//        TapInsertRecordEvent insertRecordEvent = insertRecordEvent(null, table.getId());

        //.....

        //if (null != tempList && !tempList.isEmpty()){
        //	consumer.accept(tempList,codingOffset);
        //	tempList.clear();
        //}
    }


}
