from lib.tapdata_cli.request import Api, set_req


class DataSourceApi(Api):

    url = "/Connections"


class MetadataInstancesApi(Api):

    url = "/MetadataInstances"


class DataFlowsApi(Api):

    url = "/DataFlows"


class TaskApi(Api):

    url = "/Task"


class MeasurementApi(Api):

    url = "/measurement"


class InspectApi(Api):

    url = "/task/auto-inspect-totals"
