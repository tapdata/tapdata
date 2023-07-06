###寫在前面

如果您感興趣的話，不妨前往ZoHo Desk提供的OpenAPI檔案以及WebHook檔案，詳細瞭解全部內容：

- OpenAPI檔案：[https://desk.zoho.com.cn/support/APIDocument.do#Introduction](https://desk.zoho.com.cn/support/APIDocument.do#Introduction)

- WebHook檔案：[https://desk.zoho.com.cn/support/WebhookDocument.do#Introduction](https://desk.zoho.com.cn/support/WebhookDocument.do#Introduction)

-工作流配寘檔案：[https://www.zoho.com.cn/developer/help/extensions/automation/workflow-rules.html](https://www.zoho.com.cn/developer/help/extensions/automation/workflow-rules.html)

當然您也可以瀏覽以下內容，快速上手ZoHo Desk資料來源的配寘流程。

---

### 1.内容說明

1.機构ID（org ID）：您的數據來源機构，需要您手動進入ZoHo Desk獲取並配寘到此處； 

2.用戶端ID碼（Client ID）：用戶端ID碼需要用戶前往ZoHo Desk手動獲取並複製粘貼到此；

3.用戶端機密碼（Client Secret）：用戶端機密碼與用戶端ID碼獲取管道一致，您獲取用戶端ID碼的同時也可以看到用戶端機密碼，輸入用戶端ID碼和用戶端機密碼後即可輸入應用生成碼；

4.應用生成碼（Generate Code）：應用生成碼需要與用戶端ID碼和用戶端機密碼配合使用，用於獲取OpenAPI訪問秘鑰和秘鑰重繪權杖。

5.連接模式：連接模式供用戶選擇，默認普通檔案模式，可選有普通檔案模式、CSV模式（暫未提供）。

6.增量管道：局限於ZoHo Desk的OpenAPI，ZoHo Desk資料來源僅支持WebHook增量管道，詳細的說明見下方說明。

7.服務URL：服務URL是用於配寘WebHook，需要您把此處生成的服務URL複製粘貼到ZoHo Desk的WebHook配寘項，具體的配寘流程見下方說明。

---

### 2.配寘步驟

#### 2.1基礎配寘

1.獲取**機构ID**：進入您的ZoHo Desk，點擊右上角的Setting，點擊開發者空間下的API選單，滑動到底部，您可以看到一個標題“Zoho服務通信（ZSC）金鑰”，這個表單下麵有機构ID欄位，複製這個機构ID到這裡即可。 

2.進入Api Console點擊右上角ADD CLIENT按鈕，選擇Self Client；

-點擊連結進入API Console： [https://api-console.zoho.com.cn/](https://api-console.zoho.com.cn/)

3.點擊功能表列中的Client Secret可獲取Client ID和Client Secret；

4.接下來再去獲取Generate Code，輸入Scope，輸入完整的scope有利於api獲取數據：

```
Desk.tickets.ALL,Desk.search.READ,Desk.contacts.READ,Desk.contacts.WRITE,Desk.contacts.UPDATE,Desk.contacts.CREATE,Desk.tasks.ALL,Desk.basic.READ,Desk.basic.CREATE,Desk.settings.ALL,Desk.events.ALL,Desk.articles.READ,Desk.articles.CREATE,Desk.articles.UPDATE,Desk.articles.DELETE
```

您也可以嘗試去打開以下連結前往官方檔案自己拼接合適的scope，記得用英文符號逗號分隔：

[https://desk.zoho.com.cn/support/APIDocument.do#OAuthScopes](https://desk.zoho.com.cn/support/APIDocument.do#OAuthScopes)

5.選擇一個Time Duration，可選項為3minutes、5minutes、7minutes、10minutes。 這個選項表示您接下來需要在此時間內回到TapData創建連接頁面獲取訪問Token和重繪Token。

6.點擊Create按鈕後，需要您手動選擇關聯的項目也就是ZoHo所說的門戶，選擇的門戶就是接下來數據的來源。

7.Generate Code生成後請在Time Duration配寘的這段時間內回到TapData創建連接頁面一件獲取Token，超出時間後或許希望您再次按如上步驟獲取Generate Code。

#### 2.2 WebHook配寘

配寘webHook後，您可以實現數據的時實更新。

一：全域配寘WebHook

1.第一步，您需要點擊生成服務URl按鈕生成資料來源對應的服務URL，ZoHoDesk會根據這個URL來向您傳達更新事件；

2.第二步，您需要打開您的ZoHoDesk，進入右上角Setting面板，選擇開發者空間，進入WebHook。 在選擇新建webHook。 需要您手動輸入webHook名稱，並把上一步生成的服務URL粘貼到要通知的URL輸入框。 選擇並新增您需要關注的事件。

3.點擊保存後，WebHook即生效。

二：工作流配寘Webhook

1.如果您配寘的全域WebHook無效的話，或許您需要嘗試在工作流配寘這樣一個WebHook；

2.第一步，進入Setting面板，找到“自動化”選項，選擇“工作流”選單，再選擇“規則”選單，點擊右側新建規則；

3.第二步，新建規則，選擇規則需要對應的模塊，給規則起一個響亮的名稱，然後點擊下一步；

4.第三步，選擇執行時間（我覺得應該叫執行事件，ZoHo Desk的中文翻譯不太準確），這是為了選擇觸發此工作流的操作；

5.第四步，選擇條件，可選可不選，此選項在於篩選過濾出特定的事件，如果需要的話可以選擇並設定，點擊下一步；

6.第五步，選擇一個操作，這裡我們需要配寘的是WebHook，所有您需要在選擇表格頭的左上角選擇所有操作，然後再點擊表格頭的右上角的“+”號選擇並選擇外部操作下的Send Cliq Notification；

7.第六步，編輯您的操作名稱，再降您在TapData創建ZoHo Desk資料來源時生成的服務URL粘貼到InComing WebHook URL對應得輸入框中；

8.第七步，編輯Notification Message後，點擊保存。 這樣您就完整配寘好了一個工作流。

---

### 3.錶說明

1.Tickets：工單錶。

2.Departments：部門錶。

3.Products：產品錶。

4.OrganizationFields：自訂屬性欄位錶。

5.Contracts：契约錶。

……

---

### 4.注意事項

- 您在配寘ZoHo Desk資料來源時需要生成服務URL並到ZoHo Desk中進行對應得配寘，否則增量事件將不會生效；

- 您在複製一個ZoHo Desk資料來源時同樣需要到ZoHo Desk中為其配寘新的WebHook，因為服務URL對每個ZoHo Desk資料來源來說都是獨一無二的。

- 請不要使用同一套Client ID和Client Secret創建過多ZoHo資料來源，因為同一套Client生成的訪問秘鑰是有限的，以防止全量複製過程中發生OpenAPI限流措施導致您產生不必要的損失，僅僅是ZoHo Desk只為客戶提供了少得那麼可憐的OpenAPI訪問連接數，即便您是尊貴的VIP。

- WebHook增量模式下，如若您在ZoHo Desk上删除了某條工單或者其他被WebHook監聽的數據，那麼根據ZoHo Desk的規則，您將收到一條關於更新IsDel欄位的更新事件。 但是您若是此時重置並且重啓了這個個任務，你上次删除的記錄將不在被全量讀取操作獲取，因為ZoHo Open Api不會提供IsDel欄位為TRUE的記錄。

- 關於服務URL，由於ZoHo Desk WebHook的配寘要求：你需要保證你服務的URL在80或者443埠開放，類似於 http://xxx.xx.xxx:80/xxxxxxxx ，或者 https://xxx.xx.xxx:443/xxxxxx 。 囙此，需要您在80埠或者443埠收發ZoHo Desk推送給您的數據