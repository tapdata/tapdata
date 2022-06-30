package io.tapdata.entity.utils;

/*
StringBuilder builder = new StringBuilder("InvocationCollector\r\n");
        builder.append("\tCounter: ").append(counter.longValue()).append("\r\n");
        builder.append("\tTotalTakes: ").append(totalTakes.longValue()).append("\r\n");
        boolean detailed = true;
        if(mapType != null && mapType.equalsIgnoreCase(MemoryFetcher.MEMORY_LEVEL_SUMMARY)) {
            detailed = false;
        }
        if(detailed) {
            for(Map.Entry<String, Long> entry : invokeIdTimeMap.entrySet()) {
                if(entry.getValue() != null)
                    builder.append("\tInvokeId: ").append(entry.getKey()).append(" running at: ").append(CommonUtils.dateString(new Date(entry.getValue()))).append(" used: ").append(System.currentTimeMillis() - entry.getValue()).append(" milliseconds").append("\r\n");
            }
        } else {
            builder.append("\tTotal invocation: ").append(invokeIdTimeMap.size());
        }
        return builder.toString();
 */
public class ParagraphFormatter {
    private StringBuilder builder;
    private String prefix;
    public ParagraphFormatter(String title) {
        builder = new StringBuilder(title).append("\r\n");
    }

    public ParagraphFormatter(String title, int indentation) {
        this(title);
        prefix = "";
        for(int i = 0; i < indentation; i++) {
            prefix = prefix + "\t";
        }
    }

    public ParagraphFormatter addRow(Object... kvStrings) {
        if(kvStrings != null) {
            builder.append("\t");
            if(prefix != null)
                builder.append(prefix);
            int counter = 0;
            for(Object string : kvStrings) {
                builder.append(string);
                if(counter % 2 == 0) {
                    builder.append("=");
                } else {
                    builder.append("; ");
                }
                counter++;
            }
            builder.append("\r\n");
        }
        return this;
    }

    @Override
    public String toString() {
        return builder.toString();
    }
}
