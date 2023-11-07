






授权规则及注意事项：

1）店铺类型及其状态限制：如果授权账号类型为拼多多店铺，则只允许店铺主账号才能授权成功，且店铺必须完成开店并且未发起退店申请

2）允许重复授权（当前存在频率限制，请勿密集重复授权），重复授权完成后，**历史已获取的code、access_token以及refresh_token均会立即失效**

3）允许取消授权：授权应用后，如果授权账号类型为拼多多店铺，可在拼多多服务市场-授权管理中取消授权

4）应用授权时长限制：由于不同应用使用的场景、用户角色不同、信息类型，导致其敏感度不同，我们对不同的应用标签和状态定义了不一样的授权有效时长，授权时长以获取accecss_token接口返回字段expires_at及expires_in的差值秒数为准

5）应用可授权数量限制：对于不同应用类型，其对应的可授权账号数量不同，如不限制则无需关注此影响，如果存在应用可授权数量限制，则必须在账号授权之前将账号名配置进对应要授权应用的可授权名单中，详情可在应用详情-授权管理中查看并配置

6）允许免订购授权：为方便用户使用发布至服务市场的应用，对应用新用户的授权登录将无需前置订购应用，系统将自动帮助用户完成试用动作并进入应用（前提是发布至服务市场应用已设置免费试用规格）

7）处罚及其限制影响：如您已创建的应用违反拼多多开放平台开发者规则，则将会受到新授权限制处罚，具体规则及限制情况可参考拼多多开放平台违规处理规则