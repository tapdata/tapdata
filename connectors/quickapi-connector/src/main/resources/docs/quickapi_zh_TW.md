## **Quick Api連接配寘幫助**


### 1、填寫連接名稱（必填）


第一步但不一定是第一步也可以是最後一步，要填寫連接名稱，因為這是第一個必填項。


### 2、輸入從PostMan匯出的JSON格式的API文字（必填）

匯出的JSON檔案中會包含info、item、event、variable四個主要的部分；

#### 2.1 info表示這個postman API檔案的基本資訊。


#### 2.2 item表示這個postman API檔案內包含的API介面資訊。 需要保證您為待使用API介面具有一定的編輯操作：


##### 2.2.1錶介面聲明（必要操作）


您需要在相應的錶數據API上個此API名稱上添加一些規範化的標籤，例如我使獲取ZoHo Desk上門戶的工單Tickets，那麼我需要在PostMan上對這個獲取工單的API進行一定的編輯加工：加工後的API名稱應該為

```
    TAP_TABLE**[Tickets]**（PAGE_LIMIT:data）獲取工單清單
```

其中包含了以下關鍵字：

- A、TAP_ TABLE：建錶關鍵字，表示當前API獲取到的數據會形成一張資料表。


- B、[Tickets]：指定錶名稱，一般與TAP_TABLE關鍵字一起出現，指定建錶後的錶名稱以及API獲取到的資料存儲到此錶。 使用[]包裹的一段文字。 請合理組織錶名稱，不建議使用特殊字元，如使用錶名稱中包含[]這兩個字元之一將影響建錶後的錶名稱。


- C、（PAGE_LIMIT:data）：指定獲取錶數據的分頁査詢類型，以及API調用後返回結果以data的值作為錶數據，當前API使用的是 PAGE_LIMIT 分頁類型査詢數據，表明這個API是根據記錄索引和頁內偏移進行分頁的，具體的分頁類型需要您分析API介面後進行指明，不然將會影響查詢結果，造成數據誤差。 以下是提供的分頁類型，您可以根據相關API特性進行指定分頁類型：


```

PAGE_SIZE_PAGE_INDEX：適用於使用頁碼和頁內偏移數進行分頁。 需要搭配 TAP_PAGE_SIZE 和 TAP_PAGE_INDEX 標籤指定分頁參數。

FROM_TO：適用於使用記錄開始索引和結束索引進行分頁的。 需要單配 TAP_PAGE_FROM 和 TAP_PAGE_TO 標籤指定分頁參數。

PAGE_LIMIT：適用於使用記錄索引和頁內偏移數進行分頁的。 需要搭配 TAP_PAGE_OFFSET 和 TAP_PAGE_LIMIT 標籤指定分頁參數。

PAGE_TOKEN：適用於使用緩存分頁Token進行分頁的，首頁傳空，下一頁使用上次査詢返回的token進行査詢。 需要搭配使用 TAP_PAGE_TOKEN 標籤指定分頁參數，同時使用 TAP_PAGE_SIZE 指定每次分頁査詢的記錄數，使用TAP_HAS_MORE_PAGE來描述是否有下一頁的欄位名稱（需要在參數列表中指定這個參數並在參數的描述中添加這個標籤）。


PAGE_NONE：適用於清單返回不分頁的普通數據獲取。

```


- D、分頁參數指定：以當前査詢ZoHo Desk工單API為例，使用的分頁類型為PAGE_ LIMIT，那麼分頁參數需要在其對應得描述文字中添加相應的參數標籤指明TAP_PAGE_OFFSET和TAP_PAGE_LIMIT，
```  
    TAP_ PAGE_ OFFSET則對應介面參數from，
    TAP_ PAGE_ LIMIT對應得介面參數為limit.
```
![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/quickAPI/doc/TAP_TABLE.PNG)

**補充說明：**以上是ZoHo Desk工單介面聲明的案例，Coding的獲取事項api名稱聲明案例為：


TAP_TABLE[Issues] (PAGE_SIZE_PAGE_INDEX:Response.Data.List)獲取事項清單


其語義表示為：設定了事項錶名稱為Issues，使用了PAGE_SIZE_PAGE_INDEX這個分頁邏輯，並指定了API結果中Response.Data.List的數據作為錶數據。

![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/quickAPI/doc/TAP_TABLE-2.PNG)

##### 2.2.2登入授權介面聲明


您需要使用TAP_LOGIN標籤聲明登入介面。 與錶資料介面的聲明管道一致，需要在介面名稱中添加聲明標籤，登入介面聲明標籤的關鍵字是TAP_LOGIN，使用此標籤表示此資料來源在調用API獲取數據時會進行access_token的過去判斷，那麼需要您在連接配寘頁面進行過期狀態描述以及指定access_token獲取後的鍵值匹配。， 例如下圖表示在Postman對ZoHo Desk進行登入介面的聲明：

![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/quickAPI/doc/TAP_LOGIN.PNG)

#### 2.3 event表示一些Postman事件，這個我們基本上使用不到。


#### 2.4 variable表示介面中定義的一些變數，需要保證的是在API上定義的變數一定能在這個variable中找到並存在實際且正確的值，否則這個使用了無法找到或錯誤值變數的API介面將在不久的將來調用失敗。


### 3、填寫access_ token過期狀態描述（選填）


注：

這個輸入項作為選填的原因是：部分Saas平臺提供的OpenAPI使用的是永久性的訪問權杖，無需考慮token過期的情况，例如Coding。 但對於使用臨時權杖訪問OpenAPI的Saas平臺，需要你填寫這個輸入項，否則可能造成不可預知的後果。

填寫access_token過期狀態描述。 （這裡的access_token泛指API介面訪問權杖，每個Saas的名稱可能並不一致）


- 3.1 access_token過期狀態是指您的API訪問過期後，在調用指定介面後Saas平臺返回的訪問失敗狀態。

![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/quickAPI/doc/TAP_TABLE-ZoHo.PNG)

例如我們在調用ZoHo獲取工單時，access_token過期了，此時返回結果如下圖所示，那麼您可以將過期狀態描述為errorCode=INVALID_OAUTH，這樣再執行API時可以自動根據返回結果識別為token過期實現自動刷新token。


- 3.2這個狀態描述需要您手動通過PostMan訪問API總結出來（因為我們無法預知這些Saas平臺在access_token過期後以何種響應結果返回）；


- 3.3在PostMan對登入（獲取API存取權限）的API介面進行聲明，當執行API過程中發現了access_ token過期後悔調用這個指定的API進行access_token重繪，這個登入介面需要在介面的名稱上加上TAP_GET_TOKEN這樣一個標誌性文字。 例如：對ZoHo權杖重繪介面的名稱為“TAP_GET_TOKEN重繪AccessToken-登入”，其加上了TAP_GET_TOKEN（見左上角）表示此介面用於實現自動權杖重繪操作。

![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/quickAPI/doc/TAP_LOGIN-ZoHo.PNG)

- 3.4過期狀態描述有以下描述規則：

```properties
//支持直接指定值
body.errorCode=NO AUTH

//支持且關係判斷，使用&&連接
body.errorCode=NO AUTH&&body.code=500021

//支持或關係判斷，換行處理
body.code=500021
body.code=500021

//支持範圍值
body.code=[50000,51000]

//可考慮支持規則運算式
body.errorCode=regex('$/[0-1|a-z]{5}/$')

header.httpCode=401

code=401
```

### 4、指定自動刷新token後的鍵值匹配規則（選填）


注：


這個輸入項作為選填的原因在配寘token狀態描述後需要你填寫這個輸入項，否則可能造成不可預知的後果，


雖然系統會為您在登入介面返回值中模糊匹配關鍵數據到全域參數列表中，但是無法保證模糊匹配上的token能正確賦值到你自定義的token參數上。


因為模糊匹配規則只是個經驗值，不能保證100%成功匹配上，大致的匹配思路如下：


（1）在登入授權介面的返回值中找出可能是token的欄位及其對應得值。


```
根據關鍵字access_ toke，在介面返回值中找到符合條件的token；

如果第一步沒有在則使用token關鍵字進行蒐索，如果存在多個這樣的值，那麼在全域參數中找出與訪問權杖值得格式最相近的作為訪問權杖；

如果依然沒有找到，則使用token關鍵字進行上一步操作。

最終如果沒辦法找出則拋錯並產生提示，需要手動指定返回結果與全域變數中的token鍵值規則。

```

（2）在全域參數中找出可能是訪問權杖的内容並重新賦值。

找出全域參數列表中在介面Headers中Authorization參數使用的變數。



需要指明重繪token的API在獲取到的結果中哪一個鍵值對應檔案中描述的AccessToken。


例如：

```

我在ZoHo Desk中使用Postman匯出的介面集合中，

我使用了一個全域參數accessToken來聲明了一個全域變數，

這個變數應用在所有的API上，用於API的訪問權杖。


zoho desk的登入介面返回的訪問權杖名稱叫做access_tokon，

此時我們需要在此聲明accessToken=access_token。

```


###資料來源支持


- 1.在PostMan中進行API的聲明，至少包含了一個以上的TAP_TABLE聲明後的API，否則使用這個創建的連接將無法掃描到任何錶，TAP_TABLE需要同時聲明錶名稱，分頁類型，分頁參數指定。 否則會出現錯誤的結果。


- 2.可能需要配寘登入授權API，如果配寘了登入授權API，則需要您在連接配寘頁面配寘Token過期規則以及指定獲取Token的介面中返回結果與全域變數中token變數對應關係。


- 3.支持大部分場景下的Saas資料來源，如：


使用永久Token進行OpenAPI調用的：Coding等。


使用動態重繪訪問權杖的OpenAPI調用的：ZoHo Desk等。 