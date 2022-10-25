package io.tapdata.coding.service.openApi.lamada;

import io.tapdata.coding.service.openApi.Action;
import io.tapdata.coding.utils.tool.Checker;

import java.util.Map;

/**
 * lamda
 * */
public interface HttpBodyChecker {
    public boolean checker();
    public static boolean verify(Map<String,Object> httpBody, Action action,String ... key){
        if (key.length<=0) return true;
        for (String s : key) {
            if (Checker.isEmpty(httpBody.get(s))) return false;
        }
        if (Checker.isNotEmpty(action)) action.putAction(httpBody);
        return true;
    }
}
