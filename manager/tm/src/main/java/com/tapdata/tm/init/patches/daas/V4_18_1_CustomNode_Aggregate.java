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
        dto.setName("聚合处理");
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
        return "function process(record, form){\n" +
                "  const OP_MAP = {\n" +
                "    '=': '$eq',\n" +
                "    '≠': '$ne',\n" +
                "    '>': '$gt',\n" +
                "    '≥': '$gte',\n" +
                "    '<': '$lt',\n" +
                "    '≤': '$lte',\n" +
                "    IN: '$in',\n" +
                "    'NOT IN': '$nin',\n" +
                "    REGEX: '$regex',\n" +
                "  }\n" +
                "  \n" +
                "  function buildPipelineStages(value) {\n" +
                "    const stages = []\n" +
                "  \n" +
                "    if (value.matchConditions.length > 0) {\n" +
                "      const matchObj = {}\n" +
                "      const conditions = value.matchConditions.map((c) => {\n" +
                "        const mongoOp = OP_MAP[c.operator] || '$eq'\n" +
                "        let val = c.value\n" +
                "        if (c.operator === 'IN' || c.operator === 'NOT IN') {\n" +
                "          val = val.split(',').map((s) => s.trim())\n" +
                "        }\n" +
                "        return { [c.field]: { [mongoOp]: val } }\n" +
                "      })\n" +
                "  \n" +
                "      if (conditions.length === 1) {\n" +
                "        Object.assign(matchObj, conditions[0])\n" +
                "      } else {\n" +
                "        const hasOr = value.matchConditions.some((c) => c.logic === 'OR')\n" +
                "        if (hasOr) {\n" +
                "          matchObj.$or = conditions\n" +
                "        } else {\n" +
                "          conditions.forEach((c) => Object.assign(matchObj, c))\n" +
                "        }\n" +
                "      }\n" +
                "      stages.push({ $match: matchObj })\n" +
                "    }\n" +
                "  \n" +
                "    if (value.groupFields.length > 0 || value.aggregateFields.length > 0) {\n" +
                "      const groupObj = {}\n" +
                "  \n" +
                "      if (value.groupFields.length === 1 && !value.groupFields[0]?.alias) {\n" +
                "        groupObj._id = `$${value.groupFields[0]?.field}`\n" +
                "      } else if (value.groupFields.length > 0) {\n" +
                "        groupObj._id = {}\n" +
                "        value.groupFields.forEach((g) => {\n" +
                "          const key = g.alias || g.field\n" +
                "          groupObj._id[key] = `$${g.field}`\n" +
                "        })\n" +
                "      } else {\n" +
                "        groupObj._id = null\n" +
                "      }\n" +
                "  \n" +
                "      value.aggregateFields.forEach((a) => {\n" +
                "        const opLower = a.operator.toLowerCase()\n" +
                "        if (a.operator === '$count') {\n" +
                "          groupObj[a.outputField] = { $sum: 1 }\n" +
                "        } else {\n" +
                "          groupObj[a.outputField] = { [opLower]: `$${a.sourceField}` }\n" +
                "        }\n" +
                "      })\n" +
                "  \n" +
                "      stages.push({ $group: groupObj })\n" +
                "    }\n" +
                "  \n" +
                "    return stages\n" +
                "  }\n" +
                "  \n" +
                "  function buildPipelineJSON(value, indent = 2) {\n" +
                "    if (value.useRawPipeline) {\n" +
                "      return JSON.parse(value.rawPipeline)\n" +
                "    }\n" +
                "    const stages = buildPipelineStages(value)\n" +
                "    if (stages === null) return []\n" +
                "    return stages\n" +
                "  }\n" +
                "  \n" +
                "  function resolvePipelineTemplate(template, params) {\n" +
                "    function resolveValue(value) {\n" +
                "      // 1. 字符串模板处理\n" +
                "      if (typeof value === 'string') {\n" +
                "        const match = value.match(/^\\$\\{(\\w+)\\}$/)\n" +
                "  \n" +
                "        // \uD83D\uDC49 完整占位符：${xxx}\n" +
                "        if (match) {\n" +
                "          const key = match[1]\n" +
                "          return resolveParam(params[key])\n" +
                "        }\n" +
                "  \n" +
                "        // \uD83D\uDC49 字符串中包含变量：xxx ${a} yyy\n" +
                "        return value.replace(/\\$\\{(\\w+)\\}/g, (_, key) => {\n" +
                "          const val = params[key]\n" +
                "          return val != null ? String(val) : `\\${${key}}`\n" +
                "        })\n" +
                "      }\n" +
                "  \n" +
                "      // 2. 数组\n" +
                "      if (Array.isArray(value)) {\n" +
                "        return value.map(resolveValue)\n" +
                "      }\n" +
                "  \n" +
                "      // 3. 对象\n" +
                "      if (value && typeof value === 'object') {\n" +
                "        const result = {}\n" +
                "        for (const key in value) {\n" +
                "          result[key] = resolveValue(value[key])\n" +
                "        }\n" +
                "        return result\n" +
                "      }\n" +
                "  \n" +
                "      // 4. 原始值\n" +
                "      return value\n" +
                "    }\n" +
                "  \n" +
                "    function resolveParam(val) {\n" +
                "      if (val == null) return val\n" +
                "  \n" +
                "      // \uD83D\uDC49 number / boolean 原样返回\n" +
                "      if (typeof val === 'number' || typeof val === 'boolean') {\n" +
                "        return val\n" +
                "      }\n" +
                "  \n" +
                "      // \uD83D\uDC49 Date 特殊处理\n" +
                "      if (typeof val === 'object' && val.toDate) {\n" +
                "        return {\n" +
                "          $dateTime: val\n" +
                "        }\n" +
                "      }\n" +
                "  \n" +
                "      return val\n" +
                "    }\n" +
                "  \n" +
                "    return resolveValue(template)\n" +
                "  }\n" +
                "  \n" +
                "  const { aggregate } = form\n" +
                "  const pipeline = resolvePipelineTemplate(buildPipelineJSON(aggregate), record)\n" +
                "  \n" +
                "  const db = ScriptExecutorsManager.getScriptExecutor(aggregate.connectionName)\n" +
                "  const result = db.aggregate({\n" +
                "      database: aggregate.database,\n" +
                "      collection: aggregate.tableName,\n" +
                "      pipeline\n" +
                "  });\n" +
                "  \n" +
                "  log.warn('==record==')\n" +
                "  log.warn(record)\n" +
                "  log.warn('==result==')\n" +
                "  log.warn(result)\n" +
                "  \n" +
                "  switch(context.op) {\n" +
                "    case 'd':\n" +
                "      if (aggregate.enableDeleteWhenEmpty && !result) {\n" +
                "        log.warn('==删除==')\n" +
                "        pipeline.unshift({\n" +
                "          $documents: [record]\n" +
                "        })\n" +
                "        const deleteResult = db.aggregate({\n" +
                "            database: aggregate.database,\n" +
                "            pipeline\n" +
                "        })\n" +
                "        log.warn(deleteResult[0])\n" +
                "        return deleteResult[0]\n" +
                "      } else if (result && result.length) {\n" +
                "        // 清理 before\n" +
                "        context.opList = ['u']\n" +
                "        context.before = null\n" +
                "        log.warn('==删除转更新==')\n" +
                "        return result\n" +
                "      }\n" +
                "      break\n" +
                "    case 'u':\n" +
                "      if (aggregate.effectiveUpdateFields && aggregate.effectiveUpdateFields.length) {\n" +
                "        const get = (obj, path) =>\n" +
                "          path.split('.').reduce((o, k) => (o == null ? o : o[k]), obj);\n" +
                "        \n" +
                "        const isEqual = (a, b) => {\n" +
                "          if (a === b) return true;\n" +
                "        \n" +
                "          if (typeof a !== 'object' || typeof b !== 'object' || a == null || b == null) {\n" +
                "            return false;\n" +
                "          }\n" +
                "        \n" +
                "          const keysA = Object.keys(a);\n" +
                "          const keysB = Object.keys(b);\n" +
                "          if (keysA.length !== keysB.length) return false;\n" +
                "          \n" +
                "          return keysA.every(k => isEqual(a[k], b[k]));\n" +
                "        };\n" +
                "        \n" +
                "        const before = {...context.before}\n" +
                "        const after = {...record}\n" +
                "        const hasDiff = aggregate.effectiveUpdateFields.some(path => !isEqual(get(before, path), get(after, path)));\n" +
                "        \n" +
                "        if (hasDiff) {\n" +
                "          const beforePipeline = resolvePipelineTemplate(buildPipelineJSON(aggregate), before)\n" +
                "          const beforeResult = db.aggregate({\n" +
                "              database: aggregate.database,\n" +
                "              collection: aggregate.tableName,\n" +
                "              pipeline: beforePipeline\n" +
                "          });\n" +
                "          let returnData = []\n" +
                "          let opList = []\n" +
                "          \n" +
                "          log.warn('==beforeResult==')\n" +
                "          log.warn(beforeResult)\n" +
                "          \n" +
                "          \n" +
                "          if (!beforeResult) {\n" +
                "            const deleteResult = db.aggregate({\n" +
                "              database: aggregate.database,\n" +
                "              pipeline: [{\n" +
                "                $documents: [context.before]\n" +
                "              }, ...beforePipeline]\n" +
                "            })\n" +
                "            log.warn(deleteResult)\n" +
                "            opList.push('d')\n" +
                "            returnData.push(deleteResult[0])\n" +
                "          } else {\n" +
                "            returnData.push(beforeResult[0])\n" +
                "            opList.push('i')\n" +
                "          }\n" +
                "          \n" +
                "          if (!result) {\n" +
                "            const deleteResult = db.aggregate({\n" +
                "              database: aggregate.database,\n" +
                "              pipeline: [{\n" +
                "                $documents: [record]\n" +
                "              }, ...pipeline]\n" +
                "            })\n" +
                "            opList.push('d')\n" +
                "            returnData.push(deleteResult[0])\n" +
                "          } else {\n" +
                "            returnData.push(result[0])\n" +
                "            opList.push('i')\n" +
                "          }\n" +
                "          \n" +
                "          \n" +
                "          if (opList[0] === opList[1] && isEqual({...returnData[0]}, {...returnData[1]})) {\n" +
                "            returnData = returnData[0]\n" +
                "            opList = opList[0]\n" +
                "          }\n" +
                "          \n" +
                "          log.warn('==opList==')\n" +
                "          log.warn(opList)\n" +
                "          log.warn('==returnData==')\n" +
                "          log.warn(returnData)\n" +
                "          \n" +
                "          context.opList = opList\n" +
                "          context.before = null\n" +
                "          return returnData\n" +
                "        } else {\n" +
                "          // 清理 before\n" +
                "          log.warn('清理 before')\n" +
                "          context.opList = ['u']\n" +
                "          context.before = null\n" +
                "        }\n" +
                "      }\n" +
                "      \n" +
                "      break\n" +
                "  }\n" +
                "  \n" +
                "  \n" +
                "  return result;\n" +
                "}";
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
