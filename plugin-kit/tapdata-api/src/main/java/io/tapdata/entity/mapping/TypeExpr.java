package io.tapdata.entity.mapping;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TypeExpr<T> {
    /**
     * Prefix for the expression.
     * Enhance the performance for matching by filter out the expression which prefix doesn't match.
     */
    private String prefix;
    private T value;

    public static final String KEY_PARAMS = "_params";

    /**
     * Prefix mode
     */
    public static final int PREFIX_MATCH_START = 1;
    /**
     * Prefix is the whole expression
     */
    public static final int PREFIX_MATCH_ALL = 2;
    /**
     *  Match type
     */
    private int prefixMatchType;

    private Map<String, List<Integer>> allVariableKeyOptionPosListMap = new HashMap<>();

    private AtomicInteger optionCounter = new AtomicInteger(0); //regex position 0 is the whole expression. actual param is start from 1.
    private List<Item> items;

    private String expression;
    private String regExpr;

    public boolean parseExpression(String theExpression) {
        if(theExpression == null)
            return false;
        expression = theExpression;
        //[tinyint]f[unsigned$ggg][23213$gggzero$fill]
        //tinyint[($m)][unsigned][zerofill] ==> tinyint(\((.*)\)|)(\s|)(unsigned|)(\s|)(zerofill|)
        //decimal($precision, $scale)[unsigned][zerofill]
        //date
        //varbinary($width)

        StringBuilder builder = new StringBuilder();
        items = new ArrayList<>();
//        String[] strings = expression.split("]");
        String[] strings = new String[]{theExpression};
        for(int i = 0; i < strings.length; i++) {
            String string = strings[i];
            Item item = new Item(allVariableKeyOptionPosListMap, optionCounter);
//             if(string.contains("[")) {
//                item.parseExpression(string + "]");
//            } else {
//                item.parseExpression(string);
//            }
            item.parseExpression(string);

            items.add(item);
            builder.append(item.getRegExpr());
            if(i < strings.length - 1) {
                builder.append("(\\s|)");
                optionCounter.incrementAndGet();
            }
        }
        regExpr = builder.toString();
        if(items.size() == 1) {
            if(items.get(0).isMatchAll()) {
                prefixMatchType = PREFIX_MATCH_ALL;
            } else {
                prefixMatchType = PREFIX_MATCH_START;
            }
        } else if(items.isEmpty()) {
            prefixMatchType = PREFIX_MATCH_ALL;
        } else {
            prefixMatchType = PREFIX_MATCH_START;
        }
        if(!items.isEmpty()) {
            prefix = items.get(0).getCleanPrefix();
        }
        return true;
    }

    public TypeExprResult<T> verifyValue(String value) {
        Pattern r = Pattern.compile(regExpr, Pattern.CASE_INSENSITIVE);
        Matcher m = r.matcher(value);
        boolean match = m.matches();
        if(match) {
            TypeExprResult<T> typeExprResult = new TypeExprResult<>();
            typeExprResult.setValue(this.value);
            typeExprResult.setExpression(expression);
            Map<String, String> params = new HashMap<>();
            typeExprResult.setParams(params);
            for(Map.Entry<String, List<Integer>> entry : allVariableKeyOptionPosListMap.entrySet()) {
                List<Integer> posList = entry.getValue();
                if(posList != null && !posList.isEmpty()) {
                    String matched = m.group(posList.get(0));
                    params.put(entry.getKey(), matched);
                }
            }
            return typeExprResult;
        }
        return null;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public int getPrefixMatchType() {
        return prefixMatchType;
    }

    public void setPrefixMatchType(int prefixMatchType) {
        this.prefixMatchType = prefixMatchType;
    }

    public List<Item> getItems() {
        return items;
    }

    public void setItems(List<Item> items) {
        this.items = items;
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }

    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    public Map<String, List<Integer>> getAllVariableKeyOptionPosListMap() {
        return allVariableKeyOptionPosListMap;
    }

    public void setAllVariableKeyOptionPosListMap(Map<String, List<Integer>> allVariableKeyOptionPosListMap) {
        this.allVariableKeyOptionPosListMap = allVariableKeyOptionPosListMap;
    }

    public AtomicInteger getOptionCounter() {
        return optionCounter;
    }

    public void setOptionCounter(AtomicInteger optionCounter) {
        this.optionCounter = optionCounter;
    }
}
