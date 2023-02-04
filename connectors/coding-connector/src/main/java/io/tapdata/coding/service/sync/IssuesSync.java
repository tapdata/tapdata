package io.tapdata.coding.service.sync;

import io.tapdata.coding.entity.CodingOffset;
import io.tapdata.coding.entity.ContextConfig;
import io.tapdata.coding.entity.param.IssueParam;
import io.tapdata.coding.enums.IssueType;
import io.tapdata.coding.service.loader.CodingStarter;
import io.tapdata.coding.service.loader.IssuesLoader;
import io.tapdata.coding.utils.collection.MapUtil;
import io.tapdata.coding.utils.http.HttpEntity;
import io.tapdata.coding.utils.tool.Checker;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.simplify.TapSimplify;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static io.tapdata.entity.simplify.TapSimplify.entry;
import static io.tapdata.entity.simplify.TapSimplify.map;

public class IssuesSync extends SyncAbstract implements Sync {


    public void fun(
            String TABLE_NAME,
            Long lastTimePoint,
            List<Integer> lastTimeSplitIssueCode,
            IssuesLoader loader,
            int readSizeBatch,
            Long readStartTime,
            Long readEndTime,
            int readSize,
            Object offsetState,
            BiConsumer<List<TapEvent>, Object> consumer, ContextConfig contextConfig, List<Map<String, Object>> coditions) {
        Queue<Map<String, Object>> queuePage = new ConcurrentLinkedQueue();
        AtomicBoolean pageFlag = new AtomicBoolean(true);

        Queue<Map<String, Object>> queueItem = new ConcurrentLinkedQueue();
        AtomicInteger itemThreadCount = new AtomicInteger(0);


        final List<TapEvent>[] events = new List[]{new CopyOnWriteArrayList()};
        HttpEntity<String, String> header = HttpEntity.create().builder("Authorization", contextConfig.getToken());
        String projectName = contextConfig.getProjectName();
        HttpEntity<String, Object> pageBody = HttpEntity.create()
                .builder("Action", "DescribeIssueListWithPage")
                .builder("ProjectName", projectName)
                .builder("SortKey", "UPDATED_AT")
                .builder("PageSize", readSize)
                .builder("SortValue", "ASC");
        if (Checker.isNotEmpty(contextConfig) && Checker.isNotEmpty(contextConfig.getIssueType())) {
            pageBody.builder("IssueType", IssueType.verifyType(contextConfig.getIssueType().getName()));
        } else {
            pageBody.builder("IssueType", "ALL");
        }

        String iterationCodes = contextConfig.getIterationCodes();
        if (null != iterationCodes && !"".equals(iterationCodes) && !",".equals(iterationCodes)) {
            if (!"-1".equals(iterationCodes)) {
                //-1时表示全选
                //String[] iterationCodeArr = iterationCodes.split(",");
                //@TODO 输入的迭代编号需要验证，否则，查询事项列表时作为查询条件的迭代不存在时，查询会报错
                //选择的迭代编号不需要验证
                coditions.add(map(entry("Key", "ITERATION"), entry("Value", iterationCodes)));
            }
        }
        pageBody.builder("Conditions", coditions);
        String teamName = contextConfig.getTeamName();
        if (Checker.isEmpty(offsetState)) {
            offsetState = new CodingOffset();
        }
        CodingOffset offset = (CodingOffset) offsetState;

        //分页线程
        new Thread(() -> {
            pageFlag.set(true);
            int currentQueryCount = 0, queryIndex = 0;
            int batchReadPageSize = readSizeBatch;
            do {
                /**
                 * start page ,and add page to queuePage;
                 * */
                pageBody.builder("PageNumber", queryIndex++);
                Map<String, Object> dataMap = loader.getIssuePage(header.getEntity(), pageBody.getEntity(), String.format(CodingStarter.OPEN_API_URL, teamName));
                if (null == dataMap || null == dataMap.get("List")) {
                    TapLogger.error("", "Paging result request failed, the Issue list is empty: page index = {}", queryIndex);
                    pageFlag.set(false);
                    throw new RuntimeException("Paging result request failed, the Issue list is empty: " + CodingStarter.OPEN_API_URL + "?Action=DescribeIssueListWithPage");
                }
                List<Map<String, Object>> resultList = (List<Map<String, Object>>) dataMap.get("List");
                currentQueryCount = resultList.size();
                batchReadPageSize = null != dataMap.get("PageSize") ? (int) (dataMap.get("PageSize")) : batchReadPageSize;
                queuePage.addAll(resultList);
            } while (currentQueryCount >= batchReadPageSize);

            pageFlag.set(false);
        }, "PAGE_THREAD");

        Runnable runnable = () -> {
            itemThreadCount.getAndAdd(1);
            /**
             * start page ,and add page to queuePage;
             * */
            while (!queuePage.isEmpty() || pageFlag.get()) {
                Map<String, Object> peek = queuePage.poll();
                Object code = peek.get("Code");
                Map<String, Object> issueDetail = loader.get(IssueParam.create().issueCode((Integer) code));
                if (Checker.isNotEmpty(issueDetail)) {
                    queueItem.add(issueDetail);
                }
            }
            itemThreadCount.getAndAdd(-1);
        };
        //详情查询线程
        new Thread(runnable, "ITEM_THREAD_1");
        new Thread(runnable, "ITEM_THREAD_2");

        //主线程生成事件
        while (pageFlag.get() || itemThreadCount.get() > 0 || !queuePage.isEmpty() || !queueItem.isEmpty()) {
            /**
             * 从queueItem取数据生成事件
             * **/
            Map<String, Object> issueDetail = queueItem.poll();
            if (Checker.isNotEmptyCollection(issueDetail)) {
                Long referenceTime = (Long) issueDetail.get("UpdatedAt");
                Long currentTimePoint = referenceTime - referenceTime % (24 * 60 * 60 * 1000);//时间片段
                Integer issueDetialHash = MapUtil.create().hashCode(issueDetail);

                //issueDetial的更新时间字段值是否属于当前时间片段，并且issueDiteal的hashcode是否在上一次批量读取同一时间段内
                //如果不在，说明时全新增加或修改的数据，需要在本次读取这条数据
                //如果在，说明上一次批量读取中以及读取了这条数据，本次不在需要读取 !currentTimePoint.equals(lastTimePoint) &&
                if (!lastTimeSplitIssueCode.contains(issueDetialHash)) {
                    events[0].add(TapSimplify.insertRecordEvent(issueDetail, TABLE_NAME).referenceTime(System.currentTimeMillis()));

                    if (null == currentTimePoint || !currentTimePoint.equals(lastTimePoint)) {
                        lastTimePoint = currentTimePoint;
                        lastTimeSplitIssueCode = new ArrayList<Integer>();
                    }
                    lastTimeSplitIssueCode.add(issueDetialHash);
                }

                if (Checker.isEmpty(offsetState)) {
                    offsetState = new CodingOffset();
                }
                ((CodingOffset) offsetState).getTableUpdateTimeMap().put(TABLE_NAME, referenceTime);


                if (events[0].size() == readSize) {
                    consumer.accept(events[0], offsetState);
                    events[0] = new ArrayList<>();
                }
            }
        }
    }
}
