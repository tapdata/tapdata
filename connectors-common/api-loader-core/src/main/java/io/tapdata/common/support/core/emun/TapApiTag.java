package io.tapdata.common.support.core.emun;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public enum TapApiTag {
    TAP_TABLE("TAP_TABLE",".*(TAP_TABLE\\[[^\\]]+).*","",""),
    TAP_LOGIN("TAP_LOGIN","","",""),

    PAGE_SIZE_PAGE_INDEX("PAGE_SIZE_PAGE_INDEX",".*(TAP_TABLE\\[[^\\]]+\\(PAGE_SIZE_PAGE_INDEX)).*","",""),
    FROM_TO("FROM_TO",".*(TAP_TABLE\\[[^\\]]+\\(FROM_TO)).*","",""),
    PAGE_LIMIT("PAGE_LIMIT",".*(TAP_TABLE\\[[^\\]]+\\(PAGE_LIMIT)).*","",""),
    PAGE_TOKEN("PAGE_TOKEN",".*(TAP_TABLE\\[[^\\]]+\\(PAGE_TOKEN)).*","",""),
    PAGE_NONE("PAGE_NONE",".*(TAP_TABLE\\[[^\\]]+\\(PAGE_NONE)).*","",""),

    TAP_GET_TOKEN("TAP_GET_TOKEN","","",""),
    TAP_TABLE_COUNT("TAP_TABLE_COUNT","","",""),

    TAP_PAGE_FROM("TAP_PAGE_FROM","","","TAP_PAGE_PARAM"),
    TAP_PAGE_TO("TAP_PAGE_TO","","","TAP_PAGE_PARAM"),

    TAP_PAGE_SIZE("TAP_PAGE_SIZE","","","TAP_PAGE_PARAM"),
    TAP_PAGE_INDEX("TAP_PAGE_INDEX","","","TAP_PAGE_PARAM"),

    TAP_PAGE_OFFSET("TAP_PAGE_OFFSET","","","TAP_PAGE_PARAM"),
    TAP_PAGE_LIMIT("TAP_PAGE_LIMIT","","","TAP_PAGE_PARAM"),

    TAP_PAGE_TOKEN("TAP_PAGE_TOKEN","","","TAP_PAGE_PARAM"),
    TAP_HAS_MORE_PAGE("TAP_HAS_MORE_PAGE","","","TAP_HAS_MORE_PAGE")
    ;

    String tagName;
    String tagDescription;
    String tagRegex;
    String type;
    public String type(){
        return this.type;
    }

    TapApiTag(String tagName, String tagRegex, String tagDescription,String type){
        this.tagDescription = tagDescription;
        this.tagName = tagName;
        this.tagRegex = tagRegex;
        this.type = type;
    }
    public String tagRegex(){
        return this.tagRegex;
    }
    public String tagName(){
        return this.tagName;
    }
    public String description(){
        return this.tagDescription;
    }
    public static boolean isLabeled(String apiName){
        return !labels(apiName).isEmpty();
    }
    public static List<TapApiTag> labels(String apiName){
        List<TapApiTag> labeled = new ArrayList<>();
        if (null == apiName || "".equals(apiName)) return labeled;
        TapApiTag[] tags = values();
        for (TapApiTag tag : tags) {
            if (apiName.contains(tag.tagName) && apiName.matches(tag.tagRegex)){
                labeled.add(tag);
            }
        }
        return labeled;
    }
    public static boolean hasPageStage(String apiName){
        return Objects.nonNull(getPageStage(apiName));
    }
    public static String getPageStage(String apiName){
        String suf = TAP_TABLE.tagName + "[";
        String per = "]";
        if (Objects.nonNull(apiName) && apiName.startsWith(suf)){
            int start = apiName.indexOf(suf);
            start += suf.length();
            int end = apiName.indexOf(per,start);
            if (end <= start){
                return null;
            }
            start = end + per.length();
            String pageSuf = "(";
            String pagePer = ")";
            int pageStart = apiName.indexOf(pageSuf,start);
            if (pageStart < start){
                return null;
            }
            pageStart += 1;
            int pageEnd = apiName.indexOf(pagePer,pageStart);
            if (pageEnd <= pageStart) {
                return null;
            }
            String pageStage = apiName.substring(pageStart, pageEnd);
//            return Objects.equals(PAGE_SIZE_PAGE_INDEX.tagName,pageStage)
//                    || Objects.equals(FROM_TO.tagName,pageStage)
//                    || Objects.equals(PAGE_LIMIT.tagName,pageStage)
//                    || Objects.equals(PAGE_TOKEN.tagName,pageStage)
//                    || Objects.equals(PG_NONE.tagName,pageStage)
//                    ? pageStage : null;
            return pageStage.startsWith(PAGE_SIZE_PAGE_INDEX.tagName)
                    || pageStage.startsWith(FROM_TO.tagName)
                    || pageStage.startsWith(PAGE_LIMIT.tagName)
                    || pageStage.startsWith(PAGE_TOKEN.tagName)
                    || pageStage.startsWith(PAGE_NONE.tagName)
                    ? pageStage : null;
        }
        return null;
    }

    public static boolean isTableName(String apiName){
        return Objects.nonNull(analysisTableName(apiName));
    }
    public static String analysisTableName(String apiName){
//        if (Objects.nonNull(apiName) && apiName.matches(TapApiTag.TAP_TABLE.tagRegex())){
        String suf = TAP_TABLE.tagName + "[";
        if (Objects.nonNull(apiName) && apiName.startsWith(suf)){
            int start = apiName.indexOf(suf);
            start += suf.length();
            String per = "]";
            int end = apiName.indexOf(per,start);
            if (end <= start){
                return null;
            }
            return apiName.substring(start,end);
        }
        return null;
    }

    public static boolean isTokenApi(String apiName){
        return (Objects.nonNull(apiName) && apiName.startsWith(TAP_GET_TOKEN.tagName));
    }

    public static boolean isTapPageParam(String paramDescription){
        if (Objects.isNull(paramDescription)) return false;
        TapApiTag[] values = values();
        for (TapApiTag value : values) {
            if ( "TAP_PAGE_PARAM".equals(value.type()) && paramDescription.contains(value.tagName())){
                return true;
            }
        }
        return false;
    }

    public static void main(String[] args) {

//        System.out.println(analysisTableName("TAP_TABLE[sss"));
//        System.out.println(analysisTableName("TAP_TABLE[sss]"));
//        System.out.println(analysisTableName("TAP_TABLE[]"));
//        System.out.println(analysisTableName("sssTAP_TABLE[sss"));
//        System.out.println(analysisTableName("sssTAP_TABLE[sss]"));
//        System.out.println(analysisTableName("sssTAP_TABLE[sss]eee"));
//        System.out.println(analysisTableName("TAP_TABLE[sss]eee"));
//        System.out.println(analysisTableName("TAP_TABLEddd[sss]eee"));
//
//
//        System.out.println(getPageStage("TAP_TABLE[sss(PAGE_SIZE_PAGE_INDEX)"));
//        System.out.println(getPageStage("TAP_TABLE[sss]()"));
//        System.out.println(getPageStage("TAP_TABLE[sss](SSS)"));
//        System.out.println(getPageStage("TAP_TABLE[sss](PAGE_SIZE_PAGE_INDEX)"));


        System.out.println(rangeEquals("[11]","1"));
        System.out.println(rangeEquals("[11,]","13"));
        System.out.println(rangeEquals("[,11]","1"));
        System.out.println(rangeEquals("[11,22]","12"));
    }

    private static boolean rangeEquals(String before,String after){
        //[11]    >=11
        //[11,]   >=11
        //[,11]   <=11
        //[11,22] >=11 && <=22
        int indexOfSpilt = before.indexOf(",");
        String prefixStr = indexOfSpilt==1 ? "" : before.substring(1,indexOfSpilt<0?before.indexOf("]"):indexOfSpilt);
        int indexOfSuf = before.indexOf("]");
        String suffixStr = (indexOfSpilt + 1) == indexOfSuf || indexOfSpilt <0? "" : before.substring(indexOfSpilt + 1,indexOfSuf);
        Double prefix = prefixStr.equals("")?Double.MIN_VALUE: Double.parseDouble(prefixStr);
        Double suffix = suffixStr.equals("")?Double.MAX_VALUE: Double.parseDouble(suffixStr);
        Double afterNumber = Double.valueOf(after);
        return afterNumber >= prefix && afterNumber <= suffix;
    }

}
