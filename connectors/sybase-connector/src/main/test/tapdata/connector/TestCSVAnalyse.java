package tapdata.connector;

import io.tapdata.sybase.cdc.service.AnalyseCsvFile;
import io.tapdata.sybase.util.ConnectorUtil;
import org.junit.jupiter.api.Test;

import java.util.*;

/**
 * @author GavinXiao
 * @description TestCSVAnalyse create by Gavin
 * @create 2023/7/14 11:13
 **/
public class TestCSVAnalyse {
    @Test
    public void readCSV(){
//        List<List<String>> read = AnalyseCsvFile.DEFAULT_READ_CSV.read("D:\\GavinData\\deskTop\\sybase-poc\\config\\sybase2csv\\csv\\testdb\\tester\\car_claim\\testdb.tester.car_claim.part_0.csv", null);
//        for (int index = 0; index < read.size(); index++) {
//            List<String> list = read.get(index);
//            int group = list.size() / 3;
//            for (int i = 0; i < group; i++) {
//                System.out.println(list.get(i * 3) + "  " + list.get( i * 3 + 1) + "  " + list.get(i * 3 + 2));
//            }
//        }
    }


    @Test
    public void filterAppendTable() {
        Map<String, Map<String, List<String>>> ago = new HashMap<>();
        Map<String, List<String>> a1 = new HashMap<>();
        a1.put("testdb", new ArrayList<String>(){{add("new_ccc_big7");}});
        ago.put("tester", a1);

        Map<String, Map<String, List<String>>> now = new HashMap<>();
        Map<String, List<String>> a2 = new HashMap<>();
        a2.put("testdb", new ArrayList<String>(){{add("new_ccc_big7");add("date_time1");}});
        now.put("tester", a2);

        Map<String, Map<String, List<String>>> stringMapMap = filterAppendTable(ago, now);

        System.out.println(stringMapMap.toString());
    }

    public Map<String, Map<String, List<String>>> filterAppendTable(Map<String, Map<String, List<String>>> ago, Map<String, Map<String, List<String>>> now) {
        if (null == now || now.isEmpty()) return null;
        if (null == ago || ago.isEmpty()) return now;
        Map<String, Map<String, List<String>>> appendTab = new HashMap<>();
        int appendCount = 0;
        for (Map.Entry<String, Map<String, List<String>>> tabInfo : now.entrySet()) {
            String database = tabInfo.getKey();
            Map<String, List<String>> schemaTab = tabInfo.getValue();
            if (null == schemaTab || schemaTab.isEmpty()) continue;
            Map<String, List<String>> agoSchemaInfo = ago.get(database);
            if (null == agoSchemaInfo || agoSchemaInfo.isEmpty()) {
                appendTab.put(database, schemaTab);
                continue;
            }
            Map<String, List<String>> appendSchema = new HashMap<>();
            appendTab.put(database, appendSchema);
            for (Map.Entry<String, List<String>> schemaNow : schemaTab.entrySet()) {
                String schema = schemaNow.getKey();
                List<String> tabNow = schemaNow.getValue();
                if (null == tabNow || tabNow.isEmpty()) continue;
                List<String> tabAgo = agoSchemaInfo.get(schema);
                if (null == tabAgo || tabAgo.isEmpty()) {
                    appendSchema.put(schema, tabNow);
                    continue;
                }
                Set<String> appendTableSet = new HashSet<>();
                for (String tabNameNow : tabNow) {
                    if (null == tabNameNow || "".equals(tabNameNow.trim())) continue;
                    if (!tabAgo.contains(tabNameNow)) {
                        appendTableSet.add(tabNameNow);
                        appendCount++;
                    }
                }
                appendSchema.put(schema, new ArrayList<>(appendTableSet));
            }
        }
        return appendCount > 0 ? appendTab : null;
    }

}
