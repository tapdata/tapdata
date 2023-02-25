package io.tapdata.pdk.core.api.impl;

import com.alibaba.fastjson.parser.DefaultJSONParser;
import com.alibaba.fastjson.parser.JSONScanner;
import com.alibaba.fastjson.parser.deserializer.ObjectDeserializer;
import io.tapdata.entity.utils.JsonParser;

import java.lang.reflect.Type;
import java.util.List;

public class AbstractResultDeserializer implements ObjectDeserializer {
    public static final String CHARACTERISTIC = "characteristic";

    private List<JsonParser.AbstractClassDetector> abstractClassDetectors;

    public AbstractResultDeserializer(List<JsonParser.AbstractClassDetector> abstractClassDetectors) {
        this.abstractClassDetectors = abstractClassDetectors;
    }

    @Override
    public <T> T deserialze(DefaultJSONParser parser, Type type, Object object) {
        String text = (String) parser.input; //需要反序列化的文本
        String oldText = (String) parser.input; //需要反序列化的文本
        int begin = ((JSONScanner) parser.lexer).pos()+1;//当前反序列化进行到的位置
        text = text.substring(begin,findEndPoint(text,begin)).trim().replaceAll(" ", "").replaceAll("\n", "");
        for(JsonParser.AbstractClassDetector detector : abstractClassDetectors) {
            String matchingString = detector.matchingString();

            int pos = text.indexOf(matchingString);
            if(detector.verify() && pos >= 0) {
                int charPos = pos + matchingString.length();
                boolean hit = false;
                if(charPos < text.length()) {
                    char c = text.charAt(charPos);
                    if(c == ' ' || c == ',' || c == '}') {
                        hit = true;
                    }
                } else {
                    hit = true;
                }
                if(hit) {
                    return parser.getConfig().getDeserializer(detector.getDeserializeClass()).deserialze(parser,type, object);
                }
            }
        }
        return null;
    }

   @Override
   public int getFastMatchToken() {
          return 0;
   }

   public static Integer findEndPoint(String text , int begin){
        int stack =0;
        int t;
        for (t = begin; t < text.length(); t++) {
            char ch = text.charAt(t);
            if (ch  == '{'){
                stack++;
            }
            if (ch == '}'){
                if (stack == 0){
                    break;
                }else {
                    stack--;
                }
            }
        }
        return t;
   }
}
