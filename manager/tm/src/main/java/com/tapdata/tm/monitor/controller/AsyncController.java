//package com.tapdata.tm.monitor.controller;
//
//import com.tapdata.tm.base.controller.BaseController;
//import com.tapdata.tm.base.dto.ResponseMessage;
//import com.tapdata.tm.commons.base.dto.SchedulableDto;
//import com.tapdata.tm.monitor.service.MeasurementService;
//import com.tapdata.tm.utils.GZIPUtil;
//import com.tapdata.tm.utils.UUIDUtil;
//import com.tapdata.tm.worker.service.WorkerService;
//import io.tapdata.common.async.AsyncContextManager;
//import io.tapdata.common.async.AsyncJobWaiter;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.commons.lang3.StringUtils;
//import org.apache.tomcat.util.http.fileupload.IOUtils;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.stereotype.Controller;
//import org.springframework.web.bind.annotation.RequestMapping;
//import org.springframework.web.bind.annotation.RequestMethod;
//import org.springframework.web.bind.annotation.ResponseBody;
//import org.springframework.web.multipart.MultipartFile;
//import org.springframework.web.multipart.MultipartHttpServletRequest;
//
//import javax.servlet.*;
//import javax.servlet.http.HttpServletRequest;
//import javax.servlet.http.HttpServletResponse;
//import java.io.IOException;
//import java.io.InputStream;
//import java.io.PrintWriter;
//import java.net.BindException;
//import java.nio.charset.StandardCharsets;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.concurrent.Callable;
//
//@Controller
//@RequestMapping(value = "/api/measurement")
//@Slf4j
//public class AsyncController extends BaseController {
//
//    @Autowired
//    MeasurementService measurementService;
//
//    @Autowired
//    WorkerService workerService;
//
//
//    @RequestMapping("/async/test")
//    @ResponseBody
//    public Callable<String> callable() {
//        // 调用后生成一个非web的服务线程来处理，增加web服务器的吞吐量。
//        return new Callable<String>() {
//            @Override
//            public String call() throws Exception {
//                Thread.sleep(3 * 1000L);
//                return "小单 - " + System.currentTimeMillis();
//            }
//        };
//    }
//
// /*   @RequestMapping(value="/testAsyn",method= RequestMethod.GET)
//    public void testAsny(HttpServletRequest request, HttpServletResponse response) throws IOException {
//        System.out.println("start");
//        PrintWriter out = response.getWriter();
//        AsyncContext asyncContext = request.startAsync();
//        asyncContext.setTimeout(5000);
//        asyncContext.addListener(new MyAsyncListener());
//        new Thread(new Work(asyncContext,request)).start();
//        out.print("异步执行中");
//    }*/
//
//
//    class Work implements Runnable{
//
//        private AsyncContext asyncContext;
//        private HttpServletRequest request;
//
//        public Work(AsyncContext asyncContext,HttpServletRequest request) {
//            this.asyncContext = asyncContext;
//            this.request = request;
//        }
//
//        @Override
//        public void run() {
//            try {
//                Thread.sleep(4000);
//                //此处通过观察源码，得知需要从request来判断是否超时，否则会一直抛出异常,超时的话，超时参数会置为-1
//                if(request.getAsyncContext() != null && asyncContext.getTimeout()>0){
//                    try {
//                        ServletResponse response = asyncContext.getResponse();
//                        PrintWriter out = response.getWriter();
//                        out.println("后台线程执行完成");
//                        out.close();
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
//                    asyncContext.complete();
//                }
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }
//
//    }
//
//    class MyAsyncListener implements AsyncListener {
//
//        @Override
//        public void onComplete(AsyncEvent asyncEvent) throws IOException {
//            try {
//                AsyncContext asyncContext = asyncEvent.getAsyncContext();
//                ServletResponse response = asyncContext.getResponse();
//                ServletRequest request = asyncContext.getRequest();
//                PrintWriter out= response.getWriter();
//                if (request.getAttribute("timeout") != null &&
//                        StringUtils.equals("true",request.getAttribute("timeout").toString())) {//超时
//                    out.println("后台线程执行超时---【回调】");
//                    System.out.println("异步servlet【onComplete超时】");
//                }else {//未超时
//                    out.println("后台线程执行完成---【回调】");
//                    System.out.println("异步servlet【onComplete完成】");
//                }
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//
//        @Override
//        public void onError(AsyncEvent asyncEvent) throws IOException {
//            System.out.println("异步servlet错误");
//        }
//
//        @Override
//        public void onStartAsync(AsyncEvent arg0) throws IOException {
//            System.out.println("开始异步servlet");
//        }
//
//        @Override
//        public void onTimeout(AsyncEvent asyncEvent) throws IOException {
//            ServletRequest request = asyncEvent.getAsyncContext().getRequest();
//            request.setAttribute("timeout", "true");
//            System.out.println("异步servlet【onTimeout超时】");
//        }
//
//    }
//
//    @RequestMapping(value="/downLoadLog",method= RequestMethod.POST)
//    public void downLoadLog(HttpServletRequest request, HttpServletResponse response) throws IOException {
//        AsyncContextManager asyncContextManager = AsyncContextManager.getInstance();
//        String jobId = UUIDUtil.getUUID();//返回v1
//
//        SchedulableDto schedulableDto=new SchedulableDto();
//        List<String> agentTags=new ArrayList<>();
//        agentTags.add("private");
//        schedulableDto.setAgentTags(agentTags);
//        workerService.scheduleTaskToEngine(schedulableDto,getLoginUser());
//        measurementService.startUpload(schedulableDto,jobId);
//        asyncContextManager.registerAsyncJob(jobId, request, 100,
//                (AsyncJobWaiter<InputStream>) (result, error) -> {
//                    response.setContentType("application/json");
//                    try {
//                        if(result != null) {
//                            IOUtils.copy(result, response.getOutputStream());
//                        } else {
//                            response.getOutputStream().write(error.getMessage().getBytes(StandardCharsets.UTF_8));
//                        }
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
//                });
//
//    }
//
//
//    @RequestMapping(value="/uploadStream",method= RequestMethod.POST)
//    public ResponseMessage uploadStream(HttpServletRequest request, HttpServletResponse response) throws IOException {
//        MultipartHttpServletRequest multipartRequest = (MultipartHttpServletRequest) request;
//        MultipartFile file = multipartRequest.getFile("file");
//
//        if (null==file){
//            log.error("上传文件为空");
//            throw  new BindException("Upload.File.NotExist");
//        }
//        byte[] bytes = GZIPUtil.unGzip(file.getBytes());
//        String json = new String(bytes);
//        return success();
//    }
//
//}
