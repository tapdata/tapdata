package com.tapdata.tm.init.patches.daas;

import com.tapdata.tm.customNode.dto.CustomNodeDto;
import com.tapdata.tm.customNode.service.CustomNodeService;
import com.tapdata.tm.init.PatchType;
import com.tapdata.tm.init.PatchVersion;
import com.tapdata.tm.init.patches.AbsPatch;
import com.tapdata.tm.init.patches.PatchAnnotation;
import com.tapdata.tm.utils.SpringContextHelper;
import io.tapdata.utils.AppType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.*;

@PatchAnnotation(appType = AppType.DAAS, version = "4.18-1")
public class V4_18_1_CustomNode_Aggregate extends AbsPatch {
    private static final Logger logger = LogManager.getLogger(V4_18_1_CustomNode_Aggregate.class);
    public V4_18_1_CustomNode_Aggregate(PatchType patchType, PatchVersion version) {
        super(patchType, version);
    }

    @Override
    public void run() {
        logger.info("Execute java patch: {}...", getClass().getName());
        CustomNodeService customNodeService = SpringContextHelper.getBean(CustomNodeService.class);

        CustomNodeDto dto = new CustomNodeDto();
        dto.setId(new ObjectId("69ccc354d1403e965bee8b9d"));
        dto.setName("Aggregate");
        dto.setFormSchema(buildFormSchema());
        dto.setTemplate(loadTemplate());
        dto.setCreateAt(new Date());
        dto.setCreateUser("admin@admin.com");
        dto.setLastUpdAt(new Date());
        dto.setUserId("62bc5008d4958d013d97c7a6");
        dto.setLastUpdBy("62bc5008d4958d013d97c7a6");
        Query query = new Query(Criteria.where("_id").is(new ObjectId("69ccc354d1403e965bee8b9d")));
        customNodeService.upsert(query, dto);
    }

    private String loadTemplate() {
        return """
                function process(record, form){
                  const OP_MAP = {
                    '=': '$eq',
                    '≠': '$ne',
                    '>': '$gt',
                    '≥': '$gte',
                    '<': '$lt',
                    '≤': '$lte',
                    IN: '$in',
                    'NOT IN': '$nin',
                    REGEX: '$regex',
                  }
                  const enableLog = false
                  const logWarn = msg => enableLog && log.warn(msg)
                
                  function buildPipelineStages(value) {
                    const stages = []
                
                    if (value.matchConditions.length > 0) {
                      const matchObj = {}
                      const conditions = value.matchConditions.map((c) => {
                        const mongoOp = OP_MAP[c.operator] || '$eq'
                        let val = c.value
                        if (c.operator === 'IN' || c.operator === 'NOT IN') {
                          val = val.split(',').map((s) => s.trim())
                        }
                        return { [c.field]: { [mongoOp]: val } }
                      })
                
                      if (conditions.length === 1) {
                        Object.assign(matchObj, conditions[0])
                      } else {
                        const hasOr = value.matchConditions.some((c) => c.logic === 'OR')
                        if (hasOr) {
                          matchObj.$or = conditions
                        } else {
                          conditions.forEach((c) => Object.assign(matchObj, c))
                        }
                      }
                      stages.push({ $match: matchObj })
                    }
                
                    if (value.groupFields.length > 0 || value.aggregateFields.length > 0) {
                      const groupObj = {}
                
                      if (value.groupFields.length === 1 && !value.groupFields[0]?.alias) {
                        groupObj._id = `$${value.groupFields[0]?.field}`
                      } else if (value.groupFields.length > 0) {
                        groupObj._id = {}
                        value.groupFields.forEach((g) => {
                          const key = g.alias || g.field
                          groupObj._id[key] = `$${g.field}`
                        })
                      } else {
                        groupObj._id = null
                      }
                
                      value.aggregateFields.forEach((a) => {
                        const opLower = a.operator.toLowerCase()
                        if (a.operator === '$count') {
                          groupObj[a.outputField] = { $sum: 1 }
                        } else {
                          groupObj[a.outputField] = { [opLower]: `$${a.sourceField}` }
                        }
                      })
                
                      stages.push({ $group: groupObj })
                    }
                
                    return stages
                  }
                
                  function buildPipelineJSON(value, indent = 2) {
                    if (value.useRawPipeline) {
                      return JSON.parse(value.rawPipeline)
                    }
                    const stages = buildPipelineStages(value)
                    if (stages === null) return []
                    return stages
                  }
                
                  function resolvePipelineTemplate(template, params) {
                    function resolveValue(value) {
                      if (typeof value === 'string') {
                        const match = value.match(/^\\$\\{(\\w+)\\}$/)
                        // \uD83D\uDC49 完整占位符：${xxx}
                        if (match) {
                          const key = match[1]
                          return resolveParam(params[key])
                        }
                        // \uD83D\uDC49 字符串中包含变量：xxx ${a} yyy
                        return value.replace(/\\$\\{(\\w+)\\}/g, (_, key) => {
                          const val = params[key]
                          return val != null ? String(val) : `\\${${key}}`
                        })
                      }
                
                      if (Array.isArray(value)) {
                        return value.map(resolveValue)
                      }
                
                      if (value && typeof value === 'object') {
                        const result = {}
                        for (const key in value) {
                          result[key] = resolveValue(value[key])
                        }
                        return result
                      }
                
                      return value
                    }
                
                    function resolveParam(val) {
                      if (val == null) return val
                      // \uD83D\uDC49 number / boolean 原样返回
                      if (typeof val === 'number' || typeof val === 'boolean') {
                        return val
                      }
                      // \uD83D\uDC49 Date 特殊处理
                      if (typeof val === 'object' && val.toDate) {
                        return {
                          $dateTime: val
                        }
                      }
                
                      return val
                    }
                
                    return resolveValue(template)
                  }
                
                  const { aggregate } = form
                  const pipeline = resolvePipelineTemplate(buildPipelineJSON(aggregate), record)
                  const db = ScriptExecutorsManager.getScriptExecutor(aggregate.connectionName)
                  const result = db.aggregate({
                      database: aggregate.database,
                      collection: aggregate.tableName,
                      pipeline
                  });
                
                  logWarn('==record==')
                  logWarn(record)
                  logWarn('==result==')
                  logWarn(result)
                
                  switch(context.op) {
                    case 'd':
                      if (aggregate.enableDeleteWhenEmpty && !result) {
                        logWarn('==删除==')
                        pipeline.unshift({
                          $documents: [record]
                        })
                        const deleteResult = db.aggregate({
                            database: aggregate.database,
                            pipeline
                        })
                        logWarn(deleteResult[0])
                        return deleteResult[0]
                      } else if (result && result.length) {
                        // 清理 before
                        context.opList = ['u']
                        context.before = null
                        logWarn('==删除转更新==')
                        return result
                      }
                      break
                    case 'u':
                      const get = (obj, path) =>
                        path.split('.').reduce((o, k) => (o == null ? o : o[k]), obj)
                
                      const isEqual = (a, b) => {
                        if (a === b) return true;
                
                        if (typeof a !== 'object' || typeof b !== 'object' || a == null || b == null) {
                          return false;
                        }
                
                        const keysA = Object.keys(a);
                        const keysB = Object.keys(b);
                        if (keysA.length !== keysB.length) return false;
                
                        return keysA.every(k => isEqual(a[k], b[k]));
                      }
                
                      const before = {...context.before}
                      const after = {...record}
                
                      if (aggregate.groupChangeFields && aggregate.groupChangeFields.length) {
                        const hasDiff = aggregate.groupChangeFields.some(path => !isEqual(get(before, path), get(after, path)))
                
                        if (hasDiff) {
                          const beforePipeline = resolvePipelineTemplate(buildPipelineJSON(aggregate), before)
                          const beforeResult = db.aggregate({
                              database: aggregate.database,
                              collection: aggregate.tableName,
                              pipeline: beforePipeline
                          });
                          let returnData = []
                          let opList = []
                
                          logWarn('==beforeResult==')
                          logWarn(beforeResult)
                
                
                          if (!beforeResult) {
                            const deleteResult = db.aggregate({
                              database: aggregate.database,
                              pipeline: [{
                                $documents: [context.before]
                              }, ...beforePipeline]
                            })
                            logWarn(deleteResult)
                            opList.push('d')
                            returnData.push(deleteResult[0])
                          } else {
                            returnData.push(beforeResult[0])
                            opList.push('i')
                          }
                
                          if (!result) {
                            const deleteResult = db.aggregate({
                              database: aggregate.database,
                              pipeline: [{
                                $documents: [record]
                              }, ...pipeline]
                            })
                            opList.push('d')
                            returnData.push(deleteResult[0])
                          } else {
                            returnData.push(result[0])
                            opList.push('i')
                          }
                
                
                          if (opList[0] === opList[1] && isEqual({...returnData[0]}, {...returnData[1]})) {
                            returnData = returnData.slice(1)
                            opList = opList[0].slice(1)
                          }
                
                          logWarn('==opList==')
                          logWarn(opList)
                          logWarn('==returnData==')
                          logWarn(returnData)
                
                          context.opList = opList
                          context.before = null
                          return returnData
                        }
                      }
                
                      if (aggregate.effectiveUpdateFields && aggregate.effectiveUpdateFields.length) {
                        const hasDiff = aggregate.effectiveUpdateFields.some(path => !isEqual(get(before, path), get(after, path)))
                
                        if (!hasDiff) return
                      }
                
                      // 清理 before
                      logWarn('清理 before')
                      context.opList = ['u']
                      context.before = null
                
                      break
                  }
                
                  return result
                }""";
    }

    private Map<String, Object> buildFormSchema() {
        Map<String, Object> style = new LinkedHashMap<>();
        style.put("padding", "16px");

        Map<String, Object> form = new LinkedHashMap<>();
        form.put("colon", false);
        form.put("shallow", false);
        form.put("layout", "vertical");
        form.put("feedbackLayout", "terse");
        form.put("style", style);

        Map<String, Object> aggregateDefault = new LinkedHashMap<>();
        aggregateDefault.put("useRawPipeline", false);
        aggregateDefault.put("rawPipeline", "[\n  \n]");
        aggregateDefault.put("matchConditions", Collections.emptyList());
        aggregateDefault.put("groupFields", Collections.emptyList());
        aggregateDefault.put("aggregateFields", Collections.emptyList());

        Map<String, Object> componentProps = new LinkedHashMap<>();
        componentProps.put("fieldOptions", Collections.emptyList());

        Map<String, Object> fulfill = new LinkedHashMap<>();
        fulfill.put("run", "$values.attrs.isAggregateNode = true");
        Map<String, Object> reactions = new LinkedHashMap<>();
        reactions.put("fulfill", fulfill);

        Map<String, Object> aggregate = new LinkedHashMap<>();
        aggregate.put("type", "object");
        aggregate.put("x-component", "AggregatePanel");
        aggregate.put("default", aggregateDefault);
        aggregate.put("x-component-props", componentProps);
        aggregate.put("x-reactions", reactions);
        aggregate.put("x-designable-id", "vb5215k7z9f");
        aggregate.put("x-index", 0);
        aggregate.put("name", "aggregate");

        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("aggregate", aggregate);

        Map<String, Object> schema = new LinkedHashMap<>();
        schema.put("type", "object");
        schema.put("properties", properties);
        schema.put("x-designable-id", "3tx94vk44ut");

        Map<String, Object> formSchema = new LinkedHashMap<>();
        formSchema.put("form", form);
        formSchema.put("schema", schema);
        return formSchema;
    }
}
