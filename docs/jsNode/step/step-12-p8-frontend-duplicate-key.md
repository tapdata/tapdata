# TAP-12832 P8：前端重复 key 提示

## 完成内容

- 参数编辑器新增 `duplicateScriptParamKeys` 纯函数，忽略空行并按去空格后的 key 判断重复。
- 重复参数行即时显示错误提示；参数仍可继续编辑和删除，最终保存仍由后端节点校验兜底。
- 补充中英文及繁体中文文案和纯函数测试。

## 自主审核与验证

- `node --test packages/dag/src/components/form/js-node-config-editor/script-params.test.js`：4 个测试通过。
- `pnpm exec prettier --check ...`：相关文件通过。
- `pnpm exec vue-tsc -p packages/dag/tsconfig.json --noEmit`：新增编辑器无类型错误；仓库其他既有跨包类型错误仍存在。

## 边界说明

- 即时提示只针对重复 key；key 格式、数量、值类型和大小仍由服务端 `JsNodeConfigValidator` 最终决定。
- 空 key 不被当作重复 key，但保存时会被后端判定为必填错误。
