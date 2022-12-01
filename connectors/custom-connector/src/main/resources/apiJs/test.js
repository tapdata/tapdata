function requestData(ctx){
    var page = 1;
    if(ctx.page) {
        page = ctx.page;
    }
    var headers = {
        "User-Agent": "PostmanRuntime/7.29.0",
        "accept": "*/*",
        "connection": "Keep-Alive",
        "Content-Type": "application/json",
        "Authorization": "token d7a35290cbd1314decbebc1a8a941c55b49cde27"
    };

    var url = 'https://tapdata.coding.net/open-api?Action=DescribeIssueListWithPage';
    var param = {
        "Action": "DescribeIssueListWithPage",
        "ProjectName": "tapdata",
        "IssueType": "ALL",
        "PageNumber": page,
        "PageSize": 20,
        "Conditions": []
    };
    // var result = rest.post(url, param, headers, 'object');
    // var total = result.data.Response.Data.TotalPage;
    // var array = result.data.Response.Data.List;
    // core.push(result.data.Response.Data);
    // if(page<total) {
    //     ctx.page = page+1;
    // } else {
    //     ctx.page = 1;
    // }
    // for(var msg in array) {
    //     core.push(array[msg], "i", ctx);
    // }
}