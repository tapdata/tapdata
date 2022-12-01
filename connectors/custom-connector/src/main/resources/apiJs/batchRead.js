function batchRead(connectionForm, nodeForm,offsetState, table){
    switch (table){
        case "Issues": {
            // FunctionImpl.batchReadIssues(connectionForm, nodeForm,offsetState);
        }break;
        case "Iteraration":{
            // FunctionImpl.batchReadIterarations(connectionForm, nodeForm,offsetState);
        }break;
    }
    return "hello"
}

// var STREAM_READ_TIMER = 5*60*1000;//增量读取时间间隔
// var url = 'https://tapdata.coding.net/open-api';
//
// var FunctionImpl = {
//     lastTimePointIssues : 0,
//     lastTimeSplitIssueCode : [],
//     lastTimePointIterations : 0,
//     lastTimeSplitIterationsCode : [],
//
//     hashCode : function (obj) {
//         let str = JSON.stringify(obj);
//         let hash = 0, i, chr, len;
//         if (str.length === 0) return hash;
//         for (i = 0, len = str.length; i < len; i++) {
//             chr = str.charCodeAt(i);
//             hash = ((hash << 5) - hash) + chr;
//             hash |= 0;
//         }
//         return hash;
//     },
//     builderHead:function builderHead(connectionForm){
//         return {
//             "User-Agent": "PostmanRuntime/7.29.0",
//             "accept": "*/*",
//             "connection": "Keep-Alive",
//             "Content-Type": "application/json",
//             "Authorization": "token " + connectionForm.token
//         };
//     },
//     batchReadIterarations:function batchReadIterarations(connectionForm, nodeForm,offsetState){
//         offsetState = new Date();
//         let param = {
//             "Action": "DescribeIssueListWithPage",
//             "ProjectName": "tapdata",
//             "Offset ": 1,
//             "Limit": 20,
//             "StartDate": -28800000,
//             "EndDate": offsetState.getTime()
//         };
//         let issues = [];
//         do{
//             issues = pageFun(connectionForm, nodeForm,param,offsetState);
//             param.Offset = param.Offset + 1;
//             offsetState = new Date().getTime();
//         }while(issues.length > 0)
//     },
//     batchReadIssues:function batchReadIssues(connectionForm, nodeForm,offsetState){
//         offsetState = new Date();
//         let param = {
//             "Action": "DescribeIssueListWithPage",
//             "ProjectName": "tapdata",
//             "SortKey":"CREATED_AT",
//             "PageNumber ": 1,
//             "PageSize": 20,
//             "SortValue": "ASC",
//             "Conditions": [
//                 {"Key":"CREATED_AT","Value": "1000-01-01_" + offsetState.format("yyyy-MM-dd")}
//             ]
//         };
//         let sortKeyName = "CreatedAt";
//         let issues = [];
//         do{
//             issues = pageFun(connectionForm, nodeForm,param,offsetState);
//             param.PageNumber = param.PageNumber + 1;
//             offsetState = new Date().getTime();
//         }while(issues.length > 0)
//     },
//     pageFun : function pageFun(connectionForm, nodeForm,param,offsetState){
//         let page = 1;
//         if(ctx.page) {
//             page = ctx.page;
//         }
//         let result = rest.post(url, param, this.builderHead(connectionForm), 'object');
//         let total = result.data.Response.Data.TotalPage;
//         let array = result.data.Response.Data.List;
//
//         // core.push(result.data.Response.Data);
//         ctx.page = page < total ? page+1 : 1;
//         for(let msg in array) {
//             core.push(array[msg], "i", ctx, offsetState);
//         }
//         return array;
//     },
//     streamReadIssues : function streamReadIssues(connectionForm, nodeForm,offsetState){
//         offsetState = offsetState?new Date(offsetState):new Date();
//         let param = {
//             "Action": "DescribeIssueListWithPage",
//             "ProjectName": "tapdata",
//             "SortKey":"UPDATED_AT",
//             "PageNumber ": 1,
//             "PageSize": 20,
//             "SortValue": "ASC",
//             "Conditions": [
//                 {"Key":"UPDATED_AT","Value": offsetState.format("yyyy-MM-dd")+"_"+new Date().format("yyyy-MM-dd")}
//             ]
//         };
//         let sortKeyName = "UpdatedAt";
//         let issues = [];
//         do{
//             issues = streamReadPageFun(connectionForm, nodeForm,param,offsetState);
//             for(let issue in issues) {
//                 let referenceTime = issue.UpdatedAt;
//                 let currentTimePoint = referenceTime - referenceTime % (24 * 60 * 60 * 1000);//时间片段
//                 let issueDetialHash = hashCode(issue);
//                 //issueDetial的更新时间字段值是否属于当前时间片段，并且issueDiteal的hashcode是否在上一次批量读取同一时间段内
//                 //如果不在，说明时全新增加或修改的数据，需要在本次读取这条数据
//                 //如果在，说明上一次批量读取中以及读取了这条数据，本次不在需要读取 !currentTimePoint.equals(lastTimePoint) &&
//                 if ( lastTimePointIssues !== issueDetialHash ) {
//                     core.push(issue, "i", ctx, offsetState);
//                     if ( currentTimePoint !== lastTimePointIssues ) {
//                         lastTimePointIssues = currentTimePoint;
//                         lastTimeSplitIssueCode = [];
//                     }
//                     lastTimeSplitIssueCode.push(issueDetialHash);
//                 }
//             }
//             param.PageNumber = param.PageNumber + 1;
//             offsetState = new Date().getTime();
//         }while(issues.length > 0)
//     },
//     streamReadIterarations : function streamReadIterarations(connectionForm, nodeForm,offsetState){
//         offsetState = new Date();
//         let param = {
//             "Action": "DescribeIssueListWithPage",
//             "ProjectName": "tapdata",
//             "Offset ": 1,
//             "Limit": 20,
//             "StartDate": offsetState? new Date(offsetState).getTime():new Date.getTime(),
//             "EndDate": new Date().getTime()
//         };
//         let sortKeyName = "UpdatedAt";
//         let issues = [];
//         do{
//             issues = streamReadPageFun(connectionForm, nodeForm,param,offsetState);
//             for(let issue in issues) {
//                 let referenceTime = issue.UpdatedAt;
//                 let currentTimePoint = referenceTime - referenceTime % (24 * 60 * 60 * 1000);//时间片段
//                 let iterationDetialHash = hashCode(issue);
//                 //issueDetial的更新时间字段值是否属于当前时间片段，并且issueDiteal的hashcode是否在上一次批量读取同一时间段内
//                 //如果不在，说明时全新增加或修改的数据，需要在本次读取这条数据
//                 //如果在，说明上一次批量读取中以及读取了这条数据，本次不在需要读取 !currentTimePoint.equals(lastTimePoint) &&
//                 if ( lastTimePointIterations !== iterationDetialHash ) {
//                     core.push(issue, "i", ctx, offsetState);
//                     if ( currentTimePoint !== lastTimePointIterations ) {
//                         lastTimePointIterations = currentTimePoint;
//                         lastTimeSplitIterationsCode = [];
//                     }
//                     lastTimeSplitIterationsCode.push(iterationDetialHash);
//                 }
//             }
//             param.Offset = param.Offset + 1;
//             offsetState = new Date().getTime();
//         }while(issues.length > 0)
//     },
//     streamReadPageFun : function streamReadPageFun(connectionForm, nodeForm,param,offsetState){
//         let page = 1;
//         if(ctx.page) {
//             page = ctx.page;
//         }
//         let result = rest.post(url, param, this.builderHead(connectionForm), 'object');
//         let total = result.data.Response.Data.TotalPage;
//         let array = result.data.Response.Data.List;
//         // core.push(result.data.Response.Data);
//         ctx.page = page < total ? page+1 : 1;
//         return arrar;
//     }
// }