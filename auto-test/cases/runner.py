import os, sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../init")
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from env import *

cdc_sources_config = {}
args = {}

from smart_cdc import *


# get runner test case, if not case in param, use simple sync by default
def get_case():
    if args.case is not None:
        return args.case
    return "test_dev_all.py"


# gen all possible test case params, using case description and support datasource config
def gen_run_params_template(test_case, support_datasources):
    possible_params = []
    params = test_case.test.__code__.co_varnames[0:test_case.test.__code__.co_argcount]
    for i in params:
        if i == "Pipeline":
            possible_params.append([Pipeline])
            continue

        if i == "p":
            possible_params.append(["__pipeline__"])
            continue

        if i in support_datasources:
            possible_params.append(support_datasources[i])
            continue

        logger.error("test case param not support: {}", i)
        return None

    logger.info("create case params finished, it's params is: {}, possible params is: {}", params, possible_params)

    run_params = None
    for possible_param in possible_params:
        new_run_params = []
        for param in possible_param:
            if run_params is None:
                new_run_params.append([param])
            else:
                for run_param in run_params:
                    new_run_param = deepcopy(run_param)
                    new_run_param.append(param)
                    new_run_params.append(new_run_param)
        run_params = deepcopy(new_run_params)
    return run_params


def gen_support_datasources(datasources):
    support_datasources = {}

    def init_table(k):
        if k in support_datasources:
            return
        support_datasources[k] = []

    for datasource, v in datasources.items():
        for table in v["tables"]:
            init_table(table)
            init_table(table + "_sink")
            init_table(datasource + "_" + table)
            init_table(datasource + "_" + table + "_sink")

            source_table = {
                "ns": get_test_table(datasource, table),
                "connector": v.get("config", {}).get("connector"),
                "source_name": datasource,
                "__type": v.get("config", {}).get("__type", []),
                "__core": v.get("config", {}).get("__core", False),
                "__has_data": v.get("__has_data", False),
            }
            sink_table = copy.deepcopy(source_table)
            sink_table["ns"] = get_test_table(datasource, table + "_sink")

            if v.get("config", {}).get("__core") and args.core:
                continue
            v_type = v.get("config", {}).get("__type", [])
            if "source" in v_type and v.get("__has_data"):
                if (not args.source) or args.source in [source_table["connector"], source_table["source_name"]]:
                    support_datasources[table].append(source_table)
                    support_datasources[datasource + "_" + table].append(source_table)

            if "sink" in v_type:
                if (not args.sink) or args.sink in [source_table["connector"], source_table["source_name"]]:
                    support_datasources[table + "_sink"].append(sink_table)
                    support_datasources[datasource + "_" + table + "_sink"].append(sink_table)
    return support_datasources


def run_jobs(test_case, run_params_template):
    result = []
    timeout = args.bench / 10
    if timeout < 60:
        timeout = 60

    def run_job(p, test_case, run_param):
        fn = test_case.test
        fn(*run_param)

        s = time.time()
        p.start()

        if p.wait_status("running"):
            logger.info("wait job start running cost: {} seconds", int(time.time() - s))
        else:
            logger.error("wait job running timeout: {} seconds, will skip it", int(time.time() - s))
            return False

        if not wait_job_initial(p, timeout):
            return False

        cdc_result = True
        if p.job.setting.get("type") == "initial_sync+cdc":
            cdc_result = test_cdc(p, run_param)

        stop_and_clean(p)
        check_result = manual_check(test_case, run_param)
        return cdc_result and check_result

    def gen_run_param(p, template, index):
        run_param = []
        for i in template:
            if i == "__pipeline__":
                run_param.append(p)
                continue

            if type(i) == type({}):
                sink_table = i["ns"]
                if "_sink" in i["ns"]:
                    sink_table_suffix = i["ns"].split('.')[1].split('_')[-3::2]
                    sink_table_prefix = i["ns"].split('.')[0]
                    sink_table = "%s.%s_%s_%s_%d" % (sink_table_prefix, sink_table_suffix[0], sink_table_suffix[1],
                                                     test_case.__name__, index)
                run_param.append(sink_table)
                if "cdc" in i.get("__type", []):
                    p.include_cdc()
                continue
            run_param.append(i)
        return run_param

    def wait_job_initial(p, timeout):
        s = time.time()
        if p.job.setting.get("type") == "initial_sync":
            logger.info("job is type of initial_sync, waiting for it complete")
            if not p.wait_status("complete"):
                logger.warn("job not complete, last status is: {}", p.status())
                return False
            logger.info("job complete now, wait time is: {} seconds", int(time.time() - s))
            return True
        logger.info("find job include cdc source, waiting for it into cdc")

        if not p.wait_initial_sync(quiet=False, t=timeout):
            logger.error("job NOT into sync in {}s, will skip it", timeout)
            return False

        if not p.wait_cdc_delay(quiet=False, t=timeout):
            logger.error("job NOT into make cdc delay OK in {}s, will skip it", timeout)
            return False

        logger.info("job into cdc sync and delay almost ok now, wait time is: {} seconds", int(time.time() - s))
        return True

    def test_cdc(p, run_param):
        cdc_sources = cdc_sources_config.get("cdc_sources", {})
        for param in run_param:
            if type(param) != type(""):
                continue
            if "sink_" in param:
                continue
            if param in cdc_sources:
                logger.info("find cdc source {} not support direct write, will use it's parent job as cdc source: {}",
                            param, cdc_sources[param])
                db_client = newDB(cdc_sources[param])
            else:
                db_client = newDB(param)

            time.sleep(5)  # wait 5 seconds, metrics save safety
            for event in ["insert", "delete"]:
                if not hasattr(db_client, event):
                    continue
                logger.info("now make a {} event for source: {}, and wait metrics change...", event, param)
                before = getattr(p.job.stats(), "input_" + event)
                getattr(db_client, event)()
                s = time.time()
                wait_stats = {"input_" + event: before + 1}
                if p.wait_stats(wait_stats):
                    logger.info("metrics input {} changes from {} to {}, wait time is: {} seconds", event, before,
                                wait_stats, int(time.time() - s))
                    continue
                now_stats = p.job.stats().__dict__
                logger.error("metrics input {} expect {} vs now {} wait fail after {} seconds", event, wait_stats, now_stats,
                             int(time.time() - s))
                return False

            if args.bench == 0 or not hasattr(db_client, "bench"):
                continue

            logger.info("now make a bench event for source: {}, and wait metrics change...", param)
            before = p.job.stats().input_insert
            db_client.bench(number=args.bench)
            s = time.time()
            if args.nowait:
                continue
            wait_stats = {"input_insert": before + args.bench}
            if p.wait_stats(wait_stats, t=timeout):
                wait_time = time.time() - s + 0.1
                qps = int(args.bench / wait_time)
                logger.info("metrics input insert changes from {} to {}, wait time is: {} seconds, qps is: {}", before,
                            wait_stats, int(wait_time), qps)
                continue
            now_stats = p.job.stats().input_insert
            logger.error("metrics input insert expect {} vs now {} wait fail after {} seconds", wait_stats, now_stats,
                         int(time.time() - s))
            return False
        return True

    def stop_and_clean(p):
        if args.nowait:
            return
        logger.info("stopping job {} and wait until it stopped...", p.job.name)
        s = time.time()
        p.stop()
        if p.wait_status(["stop", "complete", "error", "paused"]):
            logger.info("job status: {}, wait time {} seconds", p.job.status(), int(time.time() - s))
        else:
            logger.error("wait job stop timeout {} seconds", int(time.time() - s))

        status = p.job.status()
        stats = p.job.stats()
        logger.info(
            "job {} finish, status: {},\n" + \
            "input_stats: insert: {}, update: {}, delete: {}\n" + \
            "output_stats: insert: {}, update: {}, delete: {}",
            p.job.name, status,
            stats.input_insert, stats.input_update, stats.input_delete,
            stats.output_insert, stats.output_update, stats.output_Delete,
            "notice", "notice", "notice", "info", "info", "info", "info", "info", "info"
        )

        if args.clean:
            logger.info("cleaning job: {}", p.job.name)
            p.delete()

    def manual_check(test_case, run_param):
        if "check" not in test_case.__dict__:
            return True
        logger.info("find case check func, will run it ...")
        if test_case.check(*run_param):
            logger.info("case check {}", "success!")
            return True
        logger.error("case check {}", "fail!")
        return False

    logger.info("will create {} job for this test case and start running it...", len(run_params_template))
    for i in range(len(run_params_template)):
        try:
            logger.notice("{}", "=" * 150)
            job_name = "%s%s_%d" % (test_case.__name__, get_suffix(), i)
            case_name = get_case().split(".")[0]
            p = Pipeline(job_name, mode="sync") if "dev" in case_name else Pipeline(job_name)
            p.config({"type": "initial_sync"})
            run_param = gen_run_param(p, run_params_template[i], i)
            logger.notice("start run number {} job, name is: {}, param is: {}", i, job_name, run_param)
            with open("jobs_number", "a+") as fd:
                fd.write(".\n")
            case_result = run_job(p, test_case, run_param)
            logger.notice("{}", "#" * 150)
            if case_result:
                with open("pass_jobs_number", "a+") as fd:
                    fd.write(".\n")
            result.append({
                "params": run_param,
                "result": case_result
            })
        except Exception as e:
            print(e.with_traceback())
            result.append({
                "params": run_param,
                "result": False,
            })
    return result


def clean_smart_cdc():
    if args.nowait:
        return
    logger.info("start clean smart cdc source jobs now...")
    for p in cdc_sources_config.get("jobs", []):
        p.stop()
        p.wait_status(["stop", "complete", "error", "paused"])
        p.delete()
    logger.info("clean smart cdc source jobs done")


def main():
    case_text = "std::out >> "
    datasources = get_sources()
    global args
    args = parse_args()
    if args.smart_cdc:
        global cdc_sources_config
        cdc_sources_config = smart_cdc(datasources)

    case_name = get_case().split(".")[0]
    desc = case_name
    try:
        with open(case_name + ".py", "r") as fd:
            line = fd.readline()
            if "desc:" in line:
                desc = line.split("desc:")[1].strip()
    except Exception as e:
        pass
    case_text += "用例: " + desc
    support_datasources = gen_support_datasources(datasources)

    # logger.info("support datasource is: {}", support_datasources)
    logger.notice("find test case: {}, will make some job using it, and start running ...", case_name)
    if "test" in import_module(case_name.split(".")[0]).__dict__:
        test_case = import_module(case_name.split(".")[0])
    else:
        logger.error("no test func find in case_name, will exit")
        sys.exit(1)

    run_params_template = gen_run_params_template(test_case, support_datasources)
    result = run_jobs(test_case, run_params_template)
    clean_smart_cdc()
    success = 0
    for i in result:
        if i["result"]:
            success += 1
    case_text += ", 此用例共运行了 {} 个任务, 通过 {} 个".format(len(result), success)
    print(case_text)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--case', help="test case file, choose it from test_.* from this path", default=None)

    parser.add_argument('--source', help="datasource name, if None, will run all possible datasources", default=None)
    parser.add_argument('--sink', help="data sink name, if None, will run all possible sinks", default=None)
    parser.add_argument('--bench', help="bench cdc event number", type=int, default=0)

    parser.add_argument('--smart_cdc', help="use a tapdata job, make data load and cdc for all support datasource",
                        default=False, action='store_true')
    parser.add_argument('--clean', help="clean all data/datasource/job after case finish", default=False,
                        action='store_true')
    parser.add_argument('--core', help="only run core datasource", default=False, action='store_true')
    parser.add_argument('--nowait', help="just start job, dont wait for it finish", default=False, action='store_true')
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    main()
