package com.tapdata.tm.monitor.service;

import cn.hutool.core.date.DateUtil;
import com.mongodb.client.result.DeleteResult;
import com.tapdata.tm.BaseJunit;
import com.tapdata.tm.monitor.constant.TableNameEnum;
import com.tapdata.tm.monitor.entity.MeasurementEntity;
import com.tapdata.tm.monitor.schduler.AggregationSchedule;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

class AgentMeasurementServiceTest extends BaseJunit {

    @Autowired
    MeasurementService measurementService;
    @Autowired
    AggregationSchedule aggregationSchedule;

    @Autowired
    private MongoTemplate mongoOperations;

    /**
     * "tags" : {
     * "type" : "TM", //可观测类型， Node， Process， Agent， DataFlow， TM， ApiServer
     * "measureType" : "measureType", //指标类型
     * "processType" : "Agent", //支持3张类型的进程， 分别是Agent, TM, ApiServer
     * "customerId" : "enterpriseId", //客户ID, 如果没有可以先试用userId
     * "host" : "192.168.1.123", //主机
     * "processId" : "agent1", //进程ID
     * "agentId" : "agent1", //Agent的ID
     * "sourceId" : "source1", //数据源的ID
     * "queueId" : "queue1", //队列ID
     * "processorId" : "processor1", //数据处理器ID
     * "targetId" : "target1", //目标数据源的ID
     * "dataFlowId" : "dataFlow1" //DataFlow的ID
     * "clientId" : "alwkejrlkajflksafj", //ApiServer的clientId
     * },
     */


    @Test
    void generateMinuteInHourPoint() {
        aggregationSchedule.execute();
    }

    @Test
    void generateHourInDayPoint() {
//        agentMeasurementService.generateHourInDayPoint(new Date());
    }

    @Test
    void findInHour() {
//        agentMeasurementService.findInHour(new Date());
    }


    @Test
    void findInDay() {
//        agentMeasurementService.findInDay(new Date());
    }


    @Test
    void generateDayInMonthPoint() {
//        agentMeasurementService.generateDayInMonthPoint(new Date());
    }





    @Test
    void getPoints() {
    }

    @Test
    void testGetPoints() {
    }

    @Test
    void beforeSave() {
    }

    @Test
    public void getMinute() {
        Long d = DateUtil.nextMonth().getTime();
        d -= d % (60 * 1000);
        System.out.println(new Date(d));
    }


    @Test
    public void getEveryMinute() {
//        List list =agentMeasurementService.getEveryMinute(new Date(), DateUtil.offsetMinute(new Date(), 3));
      /*  for (Object o : list) {
            System.out.println(o);
        }*/
    }

    @Test
    public void mapSum() {
        List<Map<String, Integer>> list = new ArrayList<>();
        Map map1 = new HashMap();
        Map map2 = new HashMap();
        Map map3 = new HashMap();
        map1.put("mem", 12);
        map2.put("mem", 13);
        map3.put("mem", 14);

        list.add(map1);
        list.add(map2);
        list.add(map3);


        int max = list.stream()
                .map(map -> map.get("mem"))
                .filter(Objects::nonNull)
                .mapToInt(Integer::intValue)
                .sum();
        System.out.println(max);
    }

    @Test
    public void countKey() {
        List<Map<String, Integer>> list = new ArrayList<>();
        Map map1 = new HashMap();
        Map map2 = new HashMap();
        Map map3 = new HashMap();
        map1.put("mem", 12);
        map2.put("mem", 13);
        map3.put("mem", 14);

        list.add(map1);
        list.add(map2);
        list.add(map3);

//        List<String> nameLists = Arrays.asList("Lvshen", "Lvshen", "Zhouzhou", "Huamulan", "Huamulan", "Huamulan");

        Map<String, Long> nameMap = list.stream().map(map -> map.get("mem")).collect(Collectors.groupingBy(p -> p.toString(), Collectors.counting()));

        for (Object key : nameMap.keySet()) {
            System.out.println("key:" + key + " " + "Value:" + nameMap.get(key));

        }
    }


    private static List<Date> getDayMi(Date beagin, Date endDate) {
        Calendar tt = Calendar.getInstance();
        tt.setTime(beagin);
        Calendar t2 = Calendar.getInstance();
        t2.setTime(endDate);
//        t2.add(Calendar.DAY_OF_MONTH, 1);
        List<Date> dateList = new ArrayList<Date>();
        for (; tt.compareTo(t2) < 0; tt.add(Calendar.MINUTE, 1)) {
            dateList.add(tt.getTime());
        }
        return dateList;
    }

    @Test
    public void removeAll(){
//        DeleteResult deleteResult1 = mongoOperations.remove(new Query(),TableNameEnum.AgentMeasurement.getValue());
    }


    @Test
    public void wholeNumber(){
    }

    public static void main(String[] args) throws ParseException {
        int sampleIndex = 0;
        for (int i=0;i<5;++i){
            sampleIndex=i;
            System.out.println("sampleIndex is "+sampleIndex);
          /*  for (int j=sampleIndex;j<5;++j){
                if (j==2){
                    sampleIndex=j;
                    break;
                }
                System.out.println("sampleIndex is " +sampleIndex);
                System.out.println("j is " +j);

            }*/
        }
    }


}
