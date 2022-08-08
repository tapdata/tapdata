package io.tapdata.entity.mapping;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Splitted by "]", each of them is a Item.
 */
public class Item {
    /**
     * Origin string
     */
    private String origin;
    /**
     * The regular expression to replace variable into ".*", and more replacement.
     */
    private String regExpr;

    private String cleanPrefix;

    private Map<String, List<Integer>> allVariableKeyOptionPosListMap;

    private AtomicInteger optionCounter;

    public Item(Map<String, List<Integer>> allVariableKeyOptionPosListMap, AtomicInteger optionCounter) {
        this.allVariableKeyOptionPosListMap = allVariableKeyOptionPosListMap;
        this.optionCounter = optionCounter;
    }

    public String getOrigin() {
        return origin;
    }

    public void setOrigin(String origin) {
        this.origin = origin;
    }

    public String getRegExpr() {
        return regExpr;
    }

    public void setRegExpr(String regExpr) {
        this.regExpr = regExpr;
    }

    private static final Set<Character> variableStops = new HashSet<>();
    private static final Set<Character> regexConversions = new HashSet<>();

    static {
        variableStops.addAll(Arrays.asList('$', ' ', ',', ';', '(', ')', '.', '&', '#', '@', '!', '%', '^', '\\', '*', '-', '+', '{', '}', '|', '/', '?', '<', '>', '=', '~', '`', '[', ']'));
        regexConversions.addAll(Arrays.asList('(', ')', '!', '$', '*', '+', '.', '[', ']', '?', '\\', '/', '^', '{', '}'));
    }

    private static final char VARIABLE_START = '$';
    private static final char OPTION_START = '[';
    private static final char OPTION_END = ']';

    public void parseExpression(String string) {
        origin = string;
        //decimal($precision, $scale)[unsigned]
        //tinyint[($m)]
        //aaaa($a, $b)[23213$gggzero$fill]
        //date
        //varbinary($width)
        //tinyint[($m)][unsigned][zerofill] ==> tinyint(\((.*)\)|)(\s|)(unsigned|)(\s|)(zerofill|)

        boolean inVariableChars = false;

        StringBuilder currentVariable = new StringBuilder();
        StringBuilder builder = new StringBuilder();

        boolean stillCleanPrefix = true;
        StringBuilder prefixBuilder = new StringBuilder();

        List<Integer> optionPosList;
        for (int i = 0; i < string.length(); i++) {
            char ch = string.charAt(i);
            if(inVariableChars) { // in variable
                if(variableStops.contains(ch)) { // meet variable stop word
                    inVariableChars = false;

                    String currentV = currentVariable.toString();
                    currentVariable.delete(0, currentVariable.length());
                    optionPosList = allVariableKeyOptionPosListMap.computeIfAbsent(currentV, k -> new ArrayList<>());
                    optionPosList.add(optionCounter.get());
                } else { // still in variable, word will be ignored
                    currentVariable.append(ch);
                    continue;
                }
            }
            switch (ch) {
                case VARIABLE_START:
                    if(stillCleanPrefix) stillCleanPrefix = false;
                    if(!inVariableChars) { // if not in variable, then start
                        inVariableChars = true;
                        builder.append("(.*)"); // replace with regex
                        optionCounter.incrementAndGet();
                        continue;
                    }
                    break;
                case OPTION_START:
                    if(stillCleanPrefix) stillCleanPrefix = false;
                    builder.append("(\\s|)(");
                    optionCounter.incrementAndGet();
                    optionCounter.incrementAndGet();
                    continue;
                case OPTION_END:
                    builder.append("|)");
                    continue;
            }
            if(stillCleanPrefix) {
                prefixBuilder.append(ch);
            };
            if(regexConversions.contains(ch)) {
                builder.append("\\");
            }
            builder.append(ch);
        }
        if(inVariableChars) {
            inVariableChars = false;

            String currentV = currentVariable.toString();
            currentVariable.delete(0, currentVariable.length());
            optionPosList = allVariableKeyOptionPosListMap.computeIfAbsent(currentV, k -> new ArrayList<>());
            optionPosList.add(optionCounter.get());
        }
        cleanPrefix = prefixBuilder.toString().toLowerCase();
        regExpr = builder.toString();
    }

    public boolean isMatchAll() {
        return cleanPrefix.equals(origin) || origin.equals(regExpr);
    }

    public String getCleanPrefix() {
        return cleanPrefix;
    }

    public void setCleanPrefix(String cleanPrefix) {
        this.cleanPrefix = cleanPrefix;
    }
}
