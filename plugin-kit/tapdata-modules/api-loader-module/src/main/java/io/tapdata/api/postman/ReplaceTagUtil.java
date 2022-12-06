package io.tapdata.api.postman;

import java.util.StringJoiner;

public class ReplaceTagUtil {
    enum Tag{
        BASE_URL("\\{\\{base_url}}",""),
        ;
        String tag;
        String to;
        Tag(String tag,String to){
            this.tag = tag;
            this.to = to;
        }
        public String tag(){
            return this.tag;
        }
        public void tag(String tag){
            this.tag = tag;
        }
        public String to(){
            return this.to;
        }
        public void to(String to){
            this.to = to;
        }
        public static String tags(){
            Tag[] values = values();
            StringJoiner tags = new StringJoiner("|");
            for (Tag value : values) {
                tags.add(value.tags());
            }
            return "["+tags.toString()+"]";
        }
    }
    public static String replaceToEmpty(String itemStr){
        return itemStr.replaceAll(Tag.tags(),"");
    }
    public static String replace(String itemStr){
        if (null == itemStr ) return null;
        Tag[] values = Tag.values();
        if (values.length <= 0) return itemStr;
        for (Tag value : values) {
            itemStr = itemStr.replaceAll(value.tag(),value.to());
        }
        return itemStr;
    }
}
