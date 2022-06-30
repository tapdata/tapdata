/**
 * Copyright (c) 2021 TapData
 *
 * <p>DDL事件处理</p>
 * <ol>
 *   <li>
 *     调用方式：
 *     <ol>
 *       <li>源端：DDLProcessor.parse(DatabaseTypeEnum.MYSQL, ddl, System.out::println);</li>
 *       <li>目标端：DDLProcessor.convert(DatabaseTypeEnum.MYSQL, event, System.out::println);</li>
 *     </ol>
 *   </li>
 *   <li>
 *     开发新组件：
 *     <ol>
 *       <li>实现解析器：io.tapdata.ddlp.parsers.DDLParser</li>
 *       <li>实现转换器：io.tapdata.ddlp.converters.DDLConverter</li>
 *     </ol>
 *   </li>
 *   <li>
 *     DDL事件描述：
 *     <ol>
 *       <li>UnSupported: 未支持操作</li>
 *       <li>AddField: 添加字段</li>
 *       <li>AlterField: 修改字段</li>
 *       <li>DropField: 删除字段</li>
 *     </ol>
 *   </li>
 * </ol>
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/12/16 上午3:08 Create
 */
package io.tapdata.ddlp;
