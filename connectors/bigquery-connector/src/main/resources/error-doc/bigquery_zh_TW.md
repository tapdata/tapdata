
### 1.任務出錯自查引導

1.出錯內容包含以下文字時：

```
INVALID_ARGUMENT: Errors found while processing rows. Please refer to the row_errors field for details. The list may not be complete because of the size limitations. Entity:
```

***第一***：檢查目標錶‘已有資料處理’策略，是否保持目標錶原有結構；

***第二***：檢查生成的錶模型；

***第三***：檢查BigQuery是否存在這個錶，並查看錶結構；

***第四***：檢查任務生成的錶模型與BigQuery存在的同名表的錶結構是否一致，包括欄位及其欄位類型；

***第五***：如果不一致，為避免新生成的模型與目標端同名表不一致而產生的問題，建議將‘已處理數據’策略修改為‘清除目標端錶結構及數據’ 