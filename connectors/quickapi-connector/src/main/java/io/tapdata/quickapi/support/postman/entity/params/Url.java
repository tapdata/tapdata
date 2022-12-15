package io.tapdata.quickapi.support.postman.entity.params;

import java.util.List;
import java.util.Map;

public class Url {
    String raw;
    List<String> host;
    List<String> path;
    public static Url create(){
        return new Url();
    }
    public static Url create(Map<String,Object> map){
        try {
            String raw;
            List<String> host;
            List<String> path;
            return Url.create();
        }catch (Exception e){
            return Url.create();
        }
    }
//    public String raw(){
//        return this.raw;
//    }
//    public Url raw(String raw){
//        this.raw = raw;
//        return this;
//    }
//    public List<String> host(){
//        return this.host;
//    }
//    public Url host(String host){
//        this.host = host;
//        return this;
//    }
//    public String raw(){
//        return this.raw;
//    }
//    public Url raw(String raw){
//        this.raw = raw;
//        return this;
//    }


}
