package io.tapdata.common.postman.enums;

import java.util.StringJoiner;

public enum Symbol {
        BASE_URL("\\{\\{base_url}}",""),
        ;
        String tag;
        String to;
        Symbol(String tag, String to){
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
            Symbol[] values = values();
            StringJoiner tags = new StringJoiner("|");
            for (Symbol value : values) {
                tags.add(value.tags());
            }
            return "["+tags.toString()+"]";
        }
    }