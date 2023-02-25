# 构建并推送到Maven仓库


```shell script
# 设置版本
mvn versions:set -DnewVersion="0.0.2"

# 编译推送
mvn clean && mvn deploy -Dmaven.test.skip=true

# 还原
mvn versions:revert
```
