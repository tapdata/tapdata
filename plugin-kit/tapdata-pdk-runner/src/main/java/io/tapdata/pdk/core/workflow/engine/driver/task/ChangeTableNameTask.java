package io.tapdata.pdk.core.workflow.engine.driver.task;

import io.tapdata.entity.schema.TapTable;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * {
 *     "type" : "ChangeTableName",
 *     "tables" : [
 *      {
 *          "from" : "test",
 *          "to" : "newTestName"
 *      }
 *     ],
 *     "prefix" : "Tap_",
 *     "suffix" : "_end",
 *     "case": "lower", //lower, upper. force upper case or lower case.
 * }
 */
public class ChangeTableNameTask extends Task implements Task.TableFilter {
    private final Map<String, String> nameChangeToMap = new ConcurrentHashMap<>();
    private String prefix;
    private String suffix;
    private String letterCase;

    @Override
    protected void from(Map<String, Object> info) {
        Object tablesObj = info.get("tables");
        if(tablesObj instanceof List) {
            List<Object> tableList = (List<Object>) tablesObj;
            for(Object tableObj : tableList) {
                if(!(tableObj instanceof Map)) continue;
                Map<String, Object> tableMap = (Map<String, Object>) tableObj;
                Object fromObj = tableMap.get("from");
                Object toObj = tableMap.get("to");
                if((fromObj instanceof String) && (toObj instanceof String)) {
                    nameChangeToMap.put((String)fromObj, (String)toObj);
                }
            }
        }

        Object prefixObj = info.get("prefix");
        if(prefixObj instanceof String) {
            prefix = (String) prefixObj;
        }
        Object suffixObj = info.get("suffix");
        if(suffixObj instanceof String) {
            suffix = (String) suffixObj;
        }
        Object caseObj = info.get("case");
        if(caseObj instanceof String) {
            letterCase = (String) caseObj;
            if(!letterCase.equalsIgnoreCase("upper") && !letterCase.equalsIgnoreCase("lower")) {
                letterCase = null;
            }
        }
        if(!nameChangeToMap.isEmpty()) {
            supportTableFilter(this);
        }
    }

    @Override
    public void table(TapTable table) {
        String newName = nameChangeToMap.get(table.getId());
        if(newName != null) {
            if(table.getName().equals(table.getId())) {
              table.setName(newName);
            }
            table.setId(newName);
        }
        if(prefix != null && !table.getId().startsWith(prefix)) {
            if(table.getName().equals(table.getId())) {
                table.setName(prefix + table.getName());
            }
            table.setId(prefix + table.getId());
        }
        if(suffix != null && !table.getId().endsWith(suffix)) {
            if(table.getName().equals(table.getId())) {
                table.setName(table.getName() + suffix);
            }
            table.setId(table.getId() + suffix);
        }

        if(letterCase != null) {
            if(letterCase.equalsIgnoreCase("upper")) {
                if(table.getName().equals(table.getId())) {
                    table.setName(table.getName().toUpperCase());
                }
                table.setId(table.getId().toUpperCase());
            } else if(letterCase.equalsIgnoreCase("lower")) {
                if(table.getName().equals(table.getId())) {
                    table.setName(table.getName().toLowerCase());
                }
                table.setId(table.getId().toLowerCase());
            }
        }
    }
}
