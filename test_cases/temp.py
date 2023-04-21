from lib.tapdata_cli import cli

server = "139.198.127.204:31196"
access_code = "3324cfdf-7d3e-4792-bd32-571638d4562f"

cli.init(server, access_code)

p = cli.Pipeline(name="sync_job123", mode="sync")
p.readFrom("columns_179_1681704023194_3778.columns_179_test_dummy_dummy_source_1681704023194_3778").\
    writeTo("performance_test_dummy_target.www")

# print(p.dag.setting)

# p.stop()
