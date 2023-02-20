
### 1.任务出错自查引导

1.出错内容包含以下文本时：

```
INVALID_ARGUMENT: Errors found while processing rows. Please refer to the row_errors field for details. The list may not be complete because of the size limitations. Entity:
```

 ***第一***：检查目标表 ‘已有数据处理’ 策略，是否 保持目标表原有结构；

 ***第二***：检查生成的表模型；

 ***第三***：检查BigQuery是否存在这个表，并查看表结构；

 ***第四***：检查任务生成的表模型与BigQuery存在的同名表的表结构是否一致，包括字段及其字段类型；

 ***第五***：如果不一致，为避免新生成的模型与目标端同名表不一致而产生的问题，建议将 ‘已处理数据’ 策略修改为 ‘清除目标端表结构及数据’。
