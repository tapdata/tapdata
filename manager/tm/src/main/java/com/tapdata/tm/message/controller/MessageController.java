package com.tapdata.tm.message.controller;


import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.alibaba.fastjson.JSON;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.message.constant.MsgTypeEnum;
import com.tapdata.tm.message.constant.SystemEnum;
import com.tapdata.tm.message.dto.MessageDto;
import com.tapdata.tm.message.service.MessageService;
import com.tapdata.tm.message.vo.MessageListVo;
import com.tapdata.tm.utils.MapUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * @Date: 2021/09/13
 * @Description:
 */
@Tag(name = "Message", description = "Message 相关接口")
@RestController
@Slf4j
@RequestMapping("/api/Messages")
@Setter(onMethod_ = {@Autowired})
public class MessageController extends BaseController {
    private MessageService messageService;

    /**
     * 获取消息通知列表
     *
     * @param filterJson
     * @return
     */
    @Deprecated
    @Operation(summary = "获取消息通知列表")
    @GetMapping
    public ResponseMessage<Page<MessageListVo>> find(@RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        return success(messageService.findMessage(filter, getLoginUser()));
    }

    @Operation(summary = "get notify list")
    @GetMapping("/list")
    public ResponseMessage<Page<MessageListVo>> find(@RequestParam MsgTypeEnum msgType,
                                                     @RequestParam(required = false) String level,
                                                     @RequestParam(required = false) Boolean read,
                                                     @RequestParam(defaultValue = "1") Integer page,
                                                     @RequestParam(defaultValue = "20") Integer size) {
        return success(messageService.list(msgType, level, read, page, size, getLoginUser()));
    }

    /**
     * 获取维度消息的数字
     *
     * @param filterJson
     * @return
     */
    @GetMapping("count")
    public ResponseMessage count(@RequestParam(value = "where", required = false) String filterJson) {
        Where where = parseWhere(filterJson);
        return success(messageService.count(where, getLoginUser()));
    }

    /**
     * Find a model instance by {{id}} from the data source
     *
     * @param fieldsJson
     * @return
     */
    @Operation(summary = "Find a model instance by {{id}} from the data source")
    @GetMapping("{id}")
    public ResponseMessage<MessageDto> findById(@PathVariable("id") String id,
                                                @RequestParam("fields") String fieldsJson) {
        return success(messageService.findById(id));

    }


    /**
     * 删除单条通知
     *
     * @param whereJson
     * @return
     */
    @Operation(summary = "删除单条通知")
    @DeleteMapping
    public ResponseMessage delete(@RequestParam("where") String whereJson) {
        JSONObject jsonObject = JSONUtil.parseObj(whereJson);
        JSONArray jsonArray = jsonObject.getJSONObject("id").getJSONArray("inq");

        List<String> idList = JSONUtil.toList(jsonArray, String.class);
        Boolean result = messageService.delete(idList, getLoginUser());
        return success(result);
    }


    /**
     * Delete a model instance by {{id}} from the data source
     *
     * @return
     */
    @Operation(summary = "Delete a model instance by {{id}} from the data source")
    @DeleteMapping("deleteAll")
    public ResponseMessage deleteAll() {
        Boolean result = messageService.deleteByUserId(getLoginUser());
        return success(result);
    }

    /**
     * 将消息置为已读
     *
     * @return
     */
    @Operation(summary = "将消息置为已读")
    @PatchMapping
    public ResponseMessage read(@RequestBody MessageDto messageDto) {
        List<String> ids=new ArrayList<>();
        if(messageDto.getId() !=null) {
            ids.add(messageDto.getId().toHexString());
        }
        Boolean result = messageService.read(ids,getLoginUser());
        return success(result);
    }


    /**
     * 已读信息
     *
     * @return
     */
    @Operation(summary = "Update instances of the model matched by {{where}} from the data source")
    @PostMapping
    @RequestMapping
    public ResponseMessage read(@RequestBody(required = false) String whereFromBody,
                                @RequestParam(required = false, name = "where") String whereFromQuery) throws IOException {
        ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.currentRequestAttributes();
        HttpServletRequest request = attributes.getRequest();

        String whereJson = StringUtils.isNotBlank(whereFromQuery) ? whereFromQuery : whereFromBody;

        //todo  兼容agent传过来的请求，只能这样写，后续优化掉
        String userAgent = request.getHeader("User-Agent");
        if (StringUtils.isNotEmpty(userAgent) && (userAgent.contains("Java") || userAgent.contains("java") || userAgent.contains("nodejs"))) {
            String jsonStr = request.getReader().lines().collect(Collectors.joining(System.lineSeparator()));
            Map msgMap=JsonUtil.parseJson(jsonStr, HashMap.class);
            String jobName= MapUtils.getAsString(msgMap,"jobName");
            String sourceId=MapUtils.getAsString(msgMap,"sourceId");
            String userId=MapUtils.getAsString(msgMap,"userId");
//            messageService.addMigration(jobName,sourceId,userId);
        }
        else {
            JSONObject jsonObject = JSONUtil.parseObj(whereJson);
            if (jsonObject == null) {
                throw new BizException("InvalidParameter", "The 'where' parameter can't be empty.");
            }
            JSONArray jsonArray = null;
            if (jsonObject.getJSONObject("id") != null && jsonObject.getJSONObject("id").containsKey("inq")) {
                jsonArray = jsonObject.getJSONObject("id").getJSONArray("inq");
            }

            List<String> idList = jsonArray != null ? JSONUtil.toList(jsonArray, String.class) : null;
            if (idList != null && idList.size() > 0) {
                Boolean result = messageService.read(idList, getLoginUser());
            }
        }
        return success();
    }


    /**
     * 已读信息
     *
     * @return
     */
    @Operation(summary = "Update instances of the model matched by {{where}} from the data source")
    @PostMapping("readAll")
    public ResponseMessage readAll() {
        Boolean result = messageService.readAll(getLoginUser());
        return success(result);
    }


    /**
     * 目前只有agent的状态变动的时候，tcm调用该方法
     * @param messageDto
     * @return
     */
    @Operation(summary = "新增消息")
    @PostMapping("addMsg")
    public ResponseMessage<MessageDto> addMsg(@RequestBody MessageDto messageDto) {
        log.info("接收到新增信息请求  ,  messageDto:{}", JSON.toJSONString(messageDto));
        MessageDto messageDtoRet = new MessageDto();
        if ((SystemEnum.AGENT.getValue().equals(messageDto.getSystem()) && MsgTypeEnum.WILL_RELEASE_AGENT.getValue().equals(messageDto.getMsg()))
                || (SystemEnum.AGENT.getValue().equals(messageDto.getSystem()) && MsgTypeEnum.RELEASE_AGENT.getValue().equals(messageDto.getMsg()))) {
            //agent释放或者被释放用异步的方式做
            log.info("agent release  messageDto:{}", JsonUtil.toJson(messageDto));
            MsgTypeEnum msgTypeEnum = MsgTypeEnum.getEnumByValue(messageDto.getMsg());

            messageDtoRet = messageService.addTrustAgentMessage(messageDto.getAgentName(), messageDto.getSourceId(), msgTypeEnum, messageDto.getTitle(), getLoginUser());
        } else {
            //agent启动或者停止在这里添加通知
            messageDtoRet = messageService.add(messageDto,getLoginUser());
        }
        return success(messageDtoRet);
    }
}
