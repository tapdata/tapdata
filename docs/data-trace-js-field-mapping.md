# Data trace 适配 JS 节点字段改名说明

## 背景

Data trace 在做字段级链路匹配时，需要知道每个节点的当前字段名来自上游哪个字段。常规字段改名节点会显式维护字段映射，但 JS 节点允许用户直接写脚本，例如把 `record.pymt_mthd_intrl_nam_eng` 改名为 `pymtMthdIntrlNamEng`。如果不额外收集这类映射，数据溯源只能看到 JS 节点输出了一个新字段，无法稳定匹配它在上游链路中的来源字段。

`manager/tm-common/src/main/java/com/tapdata/tm/utils/JsFieldMapper.java` 就是为这个问题增加的轻量解析器。它不执行 JS，也不是完整 AST 解析器，而是从常见 JS 节点脚本中提取“输出字段名 -> 输入 record 字段名”的静态映射。

## 接入流程

1. `ScriptProcessNode.loadSchema` 会通过引擎推演 JS 节点输出模型，并转换成 `Schema`。
2. `ScriptProcessNode.analyseFields` 调用 `JsFieldMapper.parseMapping(script)`，得到 `Map<newField, oldField>`。
3. 对 JS 节点输出模型中的字段，如果字段名命中映射，就把来源字段写入该 `Field` 的 `originalFieldName` 和 `previousFieldName` 等元信息。
4. Data trace 构建血缘时，`FieldOriginalNameMapping.groupFieldOriginalNameMappingByNodeId` 会从节点元数据中聚合 `<nodeId, <fieldName, originalFieldName>>`。
5. `BloodlineFinder` 后续把这份映射传给 join 状态标记、更新条件字段映射、trace filter 字段过滤等逻辑，用于在字段改名前后继续匹配同一条字段链路。

需要注意：`analyseFields` 只在解析出的来源字段不再出现在当前 JS 输出模型中时，补写 `originalFieldName`/`previousFieldName`。如果脚本同时保留了原字段和新别名，当前实现不会强制覆盖已有字段元信息。

## 产品边界

本功能的定位是为 Data trace 提供 JS 节点字段改名的最佳努力匹配，不作为 JS 运行结果的完整语义分析。下面边界需要在产品语义上保持明确：

1. **静态解析优先保证可终止**：脚本静态解析出的字段映射可能和引擎推演出的输出模型不完全一致。字段转传链路如果出现重复字段，会停止继续追溯，避免模型推演或任务保存校验被循环映射阻塞。
2. **返回对象只取可解析语句**：解析器优先读取顶层 `function process(...)` 函数体中的语句级 `return`；没有顶层 `process` 函数时才读取脚本顶层 `return`。辅助函数里的 `return`、`if/for/while` 等控制块里的卫语句 `return` 不作为返回对象。可解析的返回对象仍只限简单变量或简单对象字面量。
3. **对象字面量只做外层定位**：返回变量对象字面量和直接 `return { ... }` 都会用大括号匹配读取完整外层对象，嵌套对象后面的顶层直连字段不会因为第一个 `}` 被截断；但嵌套对象、数组和函数参数内部字段不会展开，包含内部逗号的复杂字面量仍可能无法可靠分割。
4. **注释和正则只覆盖常见写法**：注释剥离会识别字符串、模板字符串和常见正则字面量，避免正则字面量中的引号影响后续 `//` 注释处理；这不是完整 JavaScript 词法器，复杂语法仍按“不可靠场景”处理。
5. **空原字段名按同名字段兜底**：字段条目存在但节点元数据没有记录 `originalFieldName` 时，Data trace filter 和流式 trace data 会把空来源字段按当前字段名处理为恒等映射，避免最终目标节点从 trace filter 结果中消失，或 `tracedFields[].originName` 返回空串。这只是缺省兜底，不代表系统推断出了额外的真实改名关系。

## JsFieldMapper 的解析规则

`parseMapping` 的返回值含义是：

```text
输出字段名 -> 来源 record 字段名
```

例如：

```javascript
const ret = {
  pymtMthdIntrlNamEng: record.pymt_mthd_intrl_nam_eng
};
return ret;
```

解析结果：

```text
pymtMthdIntrlNamEng -> pymt_mthd_intrl_nam_eng
```

解析过程分为四步：

1. 去掉行注释和块注释，避免注释里的 `return`、赋值和 `delete` 干扰解析。
2. 提取可解析的返回对象：优先在顶层 `function process(...)` 函数体中查找语句级 `return`，没有顶层 `process` 函数时再查找脚本顶层 `return`。如果返回表达式是简单变量，例如 `return ret;`，继续解析该变量；如果返回表达式是简单对象字面量，例如 `return { newField: record.oldField };`，直接解析对象内容。
3. 对 `return ret;` 这类返回变量，找到该变量的对象字面量赋值，例如 `const ret = { newField: record.oldField }`，并用大括号匹配读取完整对象体。
4. 对 `return ret;` 这类返回变量，继续扫描返回变量上的点号赋值和删除语句，例如 `ret.newField = record.oldField`、`ret.alias = ret.knownField`、`delete ret.field`。

当前实现只识别简单标识符字段名，变量名和字段名需要满足：

```text
[a-zA-Z_][a-zA-Z0-9_]*
```

## 已支持场景

### 1. 直接 return 简单对象字面量

```javascript
return {
  departmentCode: record.dept_cde,
  "paymentMethodCode": record.pymt_mthd_cde,
  'statusCode': record.status_code
};
```

解析结果：

```text
departmentCode -> dept_cde
paymentMethodCode -> pymt_mthd_cde
statusCode -> status_code
```

说明：这是用户常见的 JS 节点写法。对象 key 可以不加引号，也可以使用单引号或双引号，但 key 本身仍必须是简单标识符。对象 value 仍只支持直接读取 `record.xxx`。

### 2. 返回变量对象字面量中的直接字段改名

```javascript
const ret = {
  departmentCode: record.dept_cde,
  "paymentMethodCode": record.pymt_mthd_cde,
  'statusCode': record.status_code
};
return ret;
```

解析结果：

```text
departmentCode -> dept_cde
paymentMethodCode -> pymt_mthd_cde
statusCode -> status_code
```

说明：对象字面量的 key 可以不加引号，也可以使用单引号或双引号，但 key 本身仍必须是简单标识符。

### 3. 返回变量不带声明的对象字面量赋值

```javascript
ret = {
  fieldA: record.field_a
};
return ret;
```

解析结果：

```text
fieldA -> field_a
```

说明：对象字面量支持 `var`、`let`、`const` 声明，也支持直接给返回变量赋值。

### 4. 先创建空对象，再逐个字段赋值

```javascript
let ret = {};
ret.fieldA = record.field_a;
ret.fieldB = record.field_b
return ret;
```

解析结果：

```text
fieldA -> field_a
fieldB -> field_b
```

说明：赋值语句支持有分号结尾，也支持以换行或脚本结束作为语句边界。

### 5. 后续赋值覆盖对象字面量中的初始映射

```javascript
var ret = {
  fieldA: record.old_field_a
};
ret.fieldA = record.new_field_a;
return ret;
```

解析结果：

```text
fieldA -> new_field_a
```

说明：同一个输出字段被多次解析到时，后解析到的赋值会覆盖前面的映射。

### 6. 同一返回对象内的字段转传

```javascript
var ret = {
  sourceField: record.source_field
};
ret.aliasField = ret.sourceField;
return ret;
```

解析结果：

```text
sourceField -> source_field
aliasField -> source_field
```

说明：`ret.aliasField = ret.sourceField` 只有在 `sourceField` 已经能解析到 record 来源字段时才会生效。

### 7. 删除返回对象字段

```javascript
var ret = {
  fieldA: record.field_a,
  fieldB: record.field_b
};
delete ret.fieldA;
return ret;
```

解析结果：

```text
fieldB -> field_b
```

说明：`delete ret.fieldA` 会把 `fieldA` 从最终映射中移除。

### 8. 选择第一个可解析的顶层返回对象

```javascript
function trim(value) {
  return value;
}

function process(record) {
  if (!record.id) return;
  var ret = { newName: record.old_name };
  return ret;
}
```

解析结果：

```text
newName -> old_name
```

说明：辅助函数里的 `return value;` 和 `if` 控制块里的 `return;` 不会作为 JS 节点返回对象。对于同一搜索范围内多个可解析的顶层 `return`，仍以第一个可解析返回对象为准。

### 9. 忽略注释中的 return、赋值和 delete，并处理常见正则字面量

```javascript
var ret = {
  fieldA: record.safe_a
};
// ret.fieldB = record.comment_b;
/*
delete ret.fieldA;
return fake;
*/
return ret;
```

解析结果：

```text
fieldA -> safe_a
```

正则字面量中的引号不会影响后续注释剥离：

```javascript
var ret = {};
ret.name = record.raw_name.replace(/'/g, '');
// return record;
ret.code = record.cd;
return ret;
```

解析结果：

```text
code -> cd
```

说明：注释会先被替换为空白，注释中的脚本文本不会参与映射解析。常见位置上的正则字面量会被整体跳过，避免正则里的引号把后续脚本误判成字符串。

## 不支持或不可靠场景

下面场景源于 JS 语法本身的灵活性，当前正则解析器不会支持，或者只能得到不完整/不可靠结果。

### 1. 非简单 return 表达式不能作为返回对象

```javascript
return build(record);
```

结果：不解析。

原因：返回表达式是函数调用，不是 `return ret;` 这类简单标识符，也不是 `return { newField: record.oldField };` 这类简单对象字面量。

```javascript
var ret = { fieldA: record.field_a };
return build(record);
return ret;
```

结果：可能解析出 `fieldA -> field_a`。

原因：非简单 `return` 会被跳过，解析器会继续寻找后续可解析的顶层 `return`。这是为了容忍卫语句和辅助逻辑的静态兜底，不表达真实 JavaScript 控制流；如果复杂 `return` 在运行时先返回，后续解析结果只代表最佳努力的字段血缘。

```javascript
var ret = { fieldA: record.field_a };
var result = ret;
return result;
```

结果：不解析 `ret` 中的映射。

原因：不会追踪返回变量和其他变量之间的别名关系。

### 2. 括号访问和动态字段名

```javascript
var ret = {};
ret["payment-method"] = record["pymt_mthd_cde"];
ret[prefix + "Code"] = record.dept_cde;
return ret;
```

结果：不解析。

原因：只支持 `ret.field = record.field` 点号访问，不支持字符串 key、动态 key 或带特殊字符的字段名。

### 3. JS 标识符范围之外的字段名

```javascript
var ret = {
  "$id": record._id,
  "order-id": record.order_id,
  "中文字段": record.source_field
};
return ret;
```

结果：这些 key 不解析。

原因：字段名必须匹配 `[a-zA-Z_][a-zA-Z0-9_]*`。JS 虽然允许更多属性写法，但当前实现不支持。

### 4. 计算表达式、函数调用和多来源字段

```javascript
var ret = {};
ret.fullName = record.first_name + record.last_name;
ret.amountText = String(record.amount);
ret.safeCode = record.code || "";
return ret;
```

结果：不解析这些字段。

原因：当前只把 `record.oldField` 这种单字段直连视为可靠字段来源；计算表达式可能包含多个来源字段或语义转换，不做推断。

### 5. 中间变量、解构和 record 别名

```javascript
var ret = {};
const sourceCode = record.source_code;
ret.code = sourceCode;
return ret;
```

结果：不解析。

```javascript
var ret = {};
const { source_code } = record;
ret.code = source_code;
return ret;
```

结果：不解析。

```javascript
var row = record;
var ret = {};
ret.code = row.source_code;
return ret;
```

结果：不解析。

原因：不会做变量依赖分析，只识别右侧直接出现的 `record.xxx` 或已知的 `ret.xxx`。

### 6. 嵌套对象、数组和对象字面量中的复杂值

```javascript
var ret = {
  a: record.x,
  nested: { k: record.y },
  c: record.z
};
return ret;
```

解析结果：

```text
a -> x
c -> z
```

原因：外层对象体会用大括号匹配完整截取，所以 `nested` 后面的顶层直连字段 `c` 可以被解析；但 `nested.k` 不会展开成字段血缘。属性分割仍只是按逗号切分，不理解数组、函数参数或更深层对象中的内部逗号，这类复杂值仍不可靠。

### 7. 展开运算符、Object.assign 和批量复制

```javascript
var ret = {
  ...record,
  newCode: record.old_code
};
return ret;
```

结果：不会展开 `...record` 里的字段；简单的 `newCode: record.old_code` 可能被解析到，但无法得到完整字段链路。

```javascript
var ret = Object.assign({}, record, {
  newCode: record.old_code
});
return ret;
```

结果：不解析。

原因：当前只解析直接 `return { ... }`、返回变量的简单对象字面量赋值，以及返回变量的点号赋值。

### 8. 控制流、循环和执行顺序

```javascript
var ret = {};
if (record.type === "A") {
  ret.code = record.code_a;
} else {
  ret.code = record.code_b;
}
return ret;
```

静态解析结果会倾向于最后出现的赋值：

```text
code -> code_b
```

原因：实现不理解 `if/else` 条件，只按源码文本扫描赋值。

```javascript
var ret = {};
delete ret.code;
ret.code = record.code;
return ret;
```

当前解析结果：`code` 被删除，不会保留 `code -> code`。

原因：`delete` 在实现中作为最后一步统一处理，不表达真实 JS 执行顺序。

```javascript
var ret = {};
ret.code = record.code;
return ret;
ret.code = record.unreachable_code;
```

当前解析结果可能变为：

```text
code -> unreachable_code
```

原因：赋值扫描不会判断语句是否在 `return` 之后不可达。

### 9. 字符串或模板字符串中的代码文本

```javascript
var text = "ret.code = record.fake_code;";
var ret = {};
ret.code = record.real_code;
return ret;
```

结果：不可靠。

原因：注释剥离和返回对象定位会识别字符串边界，但后续赋值扫描仍不是完整 JavaScript 词法分析。如果字符串里恰好包含 `ret.xxx = record.xxx` 这类文本，可能被正则误识别。

### 10. Optional chaining、空值合并等现代表达式

```javascript
var ret = {};
ret.code = record?.source_code;
ret.name = record.name ?? "";
return ret;
```

结果：不解析。

原因：右侧必须是完整的 `record.identifier`，不能带 `?.`、`??`、`||` 等表达式。

## 使用建议

为了让 Data trace 能稳定识别 JS 节点中的字段改名，建议 JS 节点中涉及字段改名的部分使用下面风格：

```javascript
return {
  newField: record.old_field,
  otherNewField: record.other_old_field
};
```

或者：

```javascript
var ret = {};
ret.newField = record.old_field;
ret.otherNewField = record.other_old_field;
return ret;
```

或者：

```javascript
var ret = {
  newField: record.old_field,
  otherNewField: record.other_old_field
};
return ret;
```

如果脚本需要大量动态字段、复杂表达式、包含内部逗号的复杂对象/数组或多字段计算，当前实现无法提供完整字段来源关系；这类场景需要引入真正的 JavaScript AST/数据流分析，或者在节点配置中显式维护字段映射。
