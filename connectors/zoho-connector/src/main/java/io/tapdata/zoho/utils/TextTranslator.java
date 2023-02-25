package io.tapdata.zoho.utils;

//import com.github.houbb.html2md.util.Html2MdHelper;
//import com.overzealous.remark.Remark;
//
//import com.zoho.desk.task.TaskAPI;
//import com.zoho.oauth.client.ZohoOAuthClient;
//import com.zoho.oauth.client.ZohoOAuthTokens;
//import io.github.furstenheim.CopyDown;
//import io.github.furstenheim.Options;
//import io.github.furstenheim.OptionsBuilder;
//import org.commonmark.node.Node;
//import org.commonmark.parser.Parser;
//import org.commonmark.renderer.html.HtmlRenderer;
//
//import java.util.HashMap;
//import java.util.Map;
//import java.util.regex.Matcher;
//import java.util.regex.Pattern;

public class TextTranslator {
//    public String htmlToMarkDawn(String html){
//        Remark remark = new Remark();
//        String convert = remark.convert(html);
////        String md = Html2MdHelper.convert(html);
//        return "convert";
//    }
//
//    public static String markdownToHtml(String markdown) {
//        Parser parser = Parser.builder().build();
//        Node document = parser.parse(markdown);
//        HtmlRenderer renderer = HtmlRenderer.builder().build();
//        String result = renderer.render(document);
//        return null != result?result.replaceAll("\n",""):"";
//    }
//
//    public static String htmlToMarkdown(String htmlStr) {
//        OptionsBuilder optionsBuilder = OptionsBuilder.anOptions();
//        Options options = optionsBuilder.withBr("-")
//                // more options
//                .build();
//        CopyDown converter = new CopyDown(options);
//        String markdownText = converter.convert(htmlStr);
//        return markdownText;
//    }
//
//
//    /*** 替换指定标签的属性和值
//     *@paramstr 需要处理的字符串
//     *@paramtag 标签名称
//     *@paramtagAttrib 要替换的标签属性值
//     *@paramstartTag 新标签开始标记
//     *@paramendTag 新标签结束标记
//     *@return*@authorhuweijun
//     */
//    public static String replaceHtmlTag(String str, String tag, String tagAttrib, String newStr, Map<String,String> replaceResultMap) {
//        if(replaceResultMap==null){
//           return "";
//        }else {
//            replaceResultMap.clear();
//        }
//        String regxpForTag ="<\\s*" + tag + "\\s+([^>]*)\\s*>";
//        String regxpForTagAttrib = tagAttrib + "\\s*=\\s*[\"|']([^\"|']+)[\"|']";
//        Pattern patternForTag = Pattern.compile(regxpForTag);
//        Pattern patternForAttrib = Pattern.compile(regxpForTagAttrib);
//        Matcher matcherForTag = patternForTag.matcher(str);
//        StringBuffer sb = new StringBuffer();
//        boolean result = matcherForTag.find();
//        int replaceIndex = 0;
//        while (result) {
//            StringBuffer sbreplace = new StringBuffer("<"+tag +" ");
//            Matcher matcherForAttrib = patternForAttrib.matcher(matcherForTag.group(1));
//            if (matcherForAttrib.find()) {
//                String targetUrl = matcherForAttrib.group(1);
//                String newStringFormat = newStr+"_"+(++replaceIndex);
//                matcherForAttrib.appendReplacement(sbreplace, tagAttrib+"=\""+newStringFormat+"\"");
//                replaceResultMap.put(newStringFormat,targetUrl);
//            }
//            matcherForAttrib.appendTail(sbreplace);
//            matcherForTag.appendReplacement(sb, sbreplace.toString()+">");
//            result = matcherForTag.find();
//        }
//        matcherForTag.appendTail(sb);
//        return sb.toString();
//    }
//
//    public static void fun(){
////        TaskAPI api = TaskAPI.getInstance("2749984520@qq.com");
////        api.getTask();
//        ZohoOAuthClient  client  =  ZohoOAuthClient.getInstance();
//        String  grantToken  =  "1000.4201bf5fd4889abad20d41089a46e30e.155306a33dc4708b9720597d1ea15101";
//        ZohoOAuthTokens tokens  =  client.generateAccessToken( grantToken );
//        String  accessToken  =  tokens.getAccessToken();
//        String  refreshToken  =  tokens.getRefreshToken();
//        System.out.println(accessToken);
//        System.out.println(refreshToken);
//        System.out.println(tokens.getExpiryTime());
//
//        ZohoOAuthClient  client1  =  ZohoOAuthClient.getInstance();
//        ZohoOAuthTokens zohoOAuthTokens = client1.generateAccessTokenFromRefreshToken(refreshToken, "2749984520@qq.com");
//        System.out.println(zohoOAuthTokens.getAccessToken());
//        System.out.println(zohoOAuthTokens.getRefreshToken());
//        System.out.println(zohoOAuthTokens.getExpiryTime());
//    }
//
//    public static void main(String[] args) {
//
//        String md1= htmlToMarkdown(HTML_IMG);
//        System.out.println(md1);
//
//        String md2 = htmlToMarkdown(HTML_IMG);
//        System.out.println(md2);
//
//        String html = markdownToHtml(md2);
//        System.out.println(html);
//
//        String md3 = htmlToMarkdown(html);
//
//        System.out.println("md1 equals md2 ? "+md1.equals(md2));
//        System.out.println("HTML equals html ?"+html.equals(HTML_IMG));
//        System.out.println("md2 equals md3 ?"+md2.equals(md3));
//
//        String html1 = markdownToHtml(MD);
//        System.out.println(html1);
//        String md4 = htmlToMarkdown(html1);
//        System.out.println(md4);
//        System.out.println("MD equals md4 ?"+md4.equals(MD));
//
//        Map<String,String> replaceResultMap = new HashMap<>();
//        String targetUrl = "{{url}}";
//        String replaceUrlAfter = replaceHtmlTag(html,"img","src",targetUrl,replaceResultMap);
//        System.out.println(replaceUrlAfter);
//        String newUrl = "http://baidu.com";
//        String newUrlAfter = replaceHtmlTag(replaceUrlAfter,"img","src",newUrl,replaceResultMap);
//        System.out.println(newUrlAfter);
//
////        try {
////            fun();
////        }catch (Exception e){
////
////        }
//    }
//
//
//    public static String MD = "\\n\\u003e\\n\\n    ![](/api/km/v1/spaces/2/page/31f1b16b99124dc6a99764c42ba5f40a/blocks/fb273dcdf2a74331bebf55a3fe6afe6a/imagePreview)\\nwatch watch.you img is very big. \\n\\n# fff\\n\\n***\\u003cu\\u003e~~`sdsds#`~~\\u003c/u\\u003e***\\n";
//    public static String HTML_IMG="<blockquote style=\"border-left: 1px dotted rgb(229, 229, 229); margin-left: 5px; padding-left: 5px\" spellcheck=\"false\"><div style=\"padding-top: 10px\"><div style=\"font-size: 13px; font-family: Regular, Lato, Arial, Helvetica, sans-serif\"><div><img style=\"padding: 0px; max-width: 100%; box-sizing: border-box\" src=\"https://desk.zoho.com.cn:443/support/ImageDisplay?downloadType=uploadedFile&amp;fileName=1663898942936.png&amp;blockId=d4a8372ab5238046b67fd491ee52fd690ad7e16f5bb4b7e7&amp;zgId=7dd0ebf87c596be4dc91a236738b3d70&amp;mode=view\" /><img style=\"padding: 0px; max-width: 100%; box-sizing: border-box\" src=\"https://desk.zoho.com.cn:443/support/ImageDisplay?downloadType=uploadedFile&amp;fileName=1663898942936.png&amp;blockId=d4a8372ab5238046b67fd491ee52fd690ad7e16f5bb4b7e7&amp;zgId=7dd0ebf87c596be4dc91a236738b3d70&amp;mode=view\" /><img style=\"padding: 0px; max-width: 100%; box-sizing: border-box\" src=\"https://desk.zoho.com.cn:443/support/ImageDisplay?downloadType=uploadedFile&amp;fileName=1663898942936.png&amp;blockId=d4a8372ab5238046b67fd491ee52fd690ad7e16f5bb4b7e7&amp;zgId=7dd0ebf87c596be4dc91a236738b3d70&amp;mode=view\" /><img style=\"padding: 0px; max-width: 100%; box-sizing: border-box\" src=\"https://desk.zoho.com.cn:443/support/ImageDisplay?downloadType=uploadedFile&amp;fileName=1663898942936.png&amp;blockId=d4a8372ab5238046b67fd491ee52fd690ad7e16f5bb4b7e7&amp;zgId=7dd0ebf87c596be4dc91a236738b3d70&amp;mode=view\" />watch watch.you img is very big.</div><div></div></div></div></blockquote></div></div>";
//    public static String HTML = "<blockquote style='border-left: 1.0px dotted rgb(229,229,229);margin-left: 5.0px;padding-left: 5.0px;' spellcheck='false'><div style='padding-top: 10.0px;'><div style='font-size: 13.0px;font-family: Regular, Lato, Arial, Helvetica, sans-serif;'><div><div><div style='font-family: Regular, Lato, Arial, Helvetica, sans-serif;font-size: 13.0px;'>UAT2, 升级到1.24.33-33版本；<br /></div><div style='font-family: Regular, Lato, Arial, Helvetica, sans-serif;font-size: 13.0px;'>升级后部分任务失败调整。<br /></div><div style='font-family: Regular, Lato, Arial, Helvetica, sans-serif;font-size: 13.0px;'><br /></div><div style='font-family: Regular, Lato, Arial, Helvetica, sans-serif;font-size: 13.0px;'>UAT2环境，<br /></div><div><span class='x_587712930font' style='font-family: Regular, Lato, Arial, Helvetica, sans-serif;'><span class='x_587712930size' style='font-size: 13.0px;'>Share_Log_S_local_DAAS_4</span></span></div><div style='font-family: Regular, Lato, Arial, Helvetica, sans-serif;font-size: 13.0px;'><br /></div><div style='font-family: Regular, Lato, Arial, Helvetica, sans-serif;font-size: 13.0px;'>日志<br /></div><div><span class='x_587712930font' style='font-family: Regular, Lato, Arial, Helvetica, sans-serif;'><span class='x_587712930size' style='font-size: 13.0px;'>[ERROR]&nbsp; 2022-09-02 12:46:45 [Connector runner-Share_Log_S_local_DAAS_4_1-[62fdfe41b67a7a1067eccfae]] io.tapdata.LogCollectSource - Initial job config failed io.tapdata.exception.SourceException: Init log collector setting error, message: java.lang.String cannot be cast to java.util.Map, need stop: true.; Will stop job</span></span></div><div style='font-family: Regular, Lato, Arial, Helvetica, sans-serif;font-size: 13.0px;'><br /></div><div style='font-family: Regular, Lato, Arial, Helvetica, sans-serif;font-size: 13.0px;'>堆栈信息，<br /></div><div style='font-family: Regular, Lato, Arial, Helvetica, sans-serif;font-size: 13.0px;'><div>[ERROR] 2022-09-02 12:46:45.896 [Connector runner-Share_Log_S_local_DAAS_4_1-[62fdfe41b67a7a1067eccfae]] LogCollectSource - Initial job config failed io.tapdata.exception.SourceException: Init log collector setting error, message: java.lang.String cannot be cast to java.util.Map, need stop: true.; Will stop job</div><div>io.tapdata.exception.SourceException: io.tapdata.exception.SourceException: Init log collector setting error, message: java.lang.String cannot be cast to java.util.Map</div><div><span style='white-space: pre-wrap;'></span>at io.tapdata.common.Connector.lambda$runSource$10(Connector.java:308) ~[classes!/:0.5.2-SNAPSHOT]</div><div><span style='white-space: pre-wrap;'></span>at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149) [?:1.8.0_231]</div><div><span style='white-space: pre-wrap;'></span>at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624) [?:1.8.0_231]</div><div><span style='white-space: pre-wrap;'></span>at java.lang.Thread.run(Thread.java:748) [?:1.8.0_231]</div><div>Caused by: io.tapdata.exception.SourceException: Init log collector setting error, message: java.lang.String cannot be cast to java.util.Map</div><div><span style='white-space: pre-wrap;'></span>at io.tapdata.LogCollectSource.sourceInit(LogCollectSource.java:68) ~[log-collect-lib-0.5.2-SNAPSHOT.jar!/:0.5.2-SNAPSHOT]</div><div><span style='white-space: pre-wrap;'></span>at io.tapdata.common.Connector.lambda$runSource$10(Connector.java:304) ~[classes!/:0.5.2-SNAPSHOT]</div><div><span style='white-space: pre-wrap;'></span>... 3 more</div><div>Caused by: java.lang.ClassCastException: java.lang.String cannot be cast to java.util.Map</div><div><span style='white-space: pre-wrap;'></span>at io.tapdata.LogCollectSource.lambda$initLogCollectorSettings$0(LogCollectSource.java:108) ~[log-collect-lib-0.5.2-SNAPSHOT.jar!/:0.5.2-SNAPSHOT]</div><div><span style='white-space: pre-wrap;'></span>at java.util.Iterator.forEachRemaining(Iterator.java:116) ~[?:1.8.0_231]</div><div><span style='white-space: pre-wrap;'></span>at java.util.Spliterators$IteratorSpliterator.forEachRemaining(Spliterators.java:1801) ~[?:1.8.0_231]</div><div><span style='white-space: pre-wrap;'></span>at java.util.stream.ReferencePipeline$Head.forEach(ReferencePipeline.java:580) ~[?:1.8.0_231]</div><div><span style='white-space: pre-wrap;'></span>at io.tapdata.LogCollectSource.initLogCollectorSettings(LogCollectSource.java:108) ~[log-collect-lib-0.5.2-SNAPSHOT.jar!/:0.5.2-SNAPSHOT]</div><div><span style='white-space: pre-wrap;'></span>at io.tapdata.LogCollectSource.sourceInit(LogCollectSource.java:66) ~[log-collect-lib-0.5.2-SNAPSHOT.jar!/:0.5.2-SNAPSHOT]</div><div><span style='white-space: pre-wrap;'></span>at io.tapdata.common.Connector.lambda$runSource$10(Connector.java:304) ~[classes!/:0.5.2-SNAPSHOT]</div><div><span style='white-space: pre-wrap;'></span>... 3 more</div></div></div><div style='font-family: Regular, Lato, Arial, Helvetica, sans-serif;font-size: 13.0px;'><br /></div></div><div><br /></div></div></div></blockquote></div> <div><br /></div></div><div id='x_587712930ZDeskInteg'><meta itemprop='zdeskTicket' content='391b0ce48280e0e10eba0c3e30e6fae0b2049e53c3f2872772e8e3547298bef09f8292aa4bcc25a081a1de620d4616173f4c55ab51756b600f1b322bb7b47b52' /></div><br /></div>";
}
