package io.tapdata.sybase.cdc.service;

import io.tapdata.entity.error.CoreException;
import io.tapdata.sybase.cdc.CdcRoot;
import io.tapdata.sybase.cdc.CdcStep;
import io.tapdata.sybase.cdc.dto.start.CommandType;

/**
 * @author GavinXiao
 * @description ExecCommand create by Gavin
 * @create 2023/7/13 11:17
 **/
class ExecCommand implements CdcStep<CdcRoot> {
    private CdcRoot root;
    private CommandType commandType;

    private final static String EXPORT_JAVA_HOME = "export JAVA_TOOL_OPTIONS=\"-Duser.language=en\"";
    private final static String START_CDC = "$pocPath$/replicant-cli/bin/replicant $commandType$ $pocPath$/config/sybase2csv/src_sybasease.yaml $pocPath$/config/sybase2csv/dst_localstorage.yaml --general $pocPath$/config/sybase2csv/general.yaml --filter $pocPath$/config/sybase2csv/filter_sybasease.yaml --extractor $pocPath$/config/sybase2csv/ext_sybasease.yaml --id tstcsv1 --replace --overwrite --verbose";

    protected ExecCommand (CdcRoot root, CommandType commandType) {
        this.root = root;
        this.commandType = commandType;
    }


    @Override
    public CdcRoot compile() {
        String cmd = START_CDC.replaceAll("\\$pocPath\\$", root.getCdcFile().getAbsolutePath()).replaceAll("\\$commandType\\$", CommandType.type(commandType));
        try {
            Runtime runtime = Runtime.getRuntime();
            root.setProcess(runtime.exec(cmd));
        }catch (Exception e){
            throw new CoreException("Command exec failed, unable to start cdc command: {}, msg: {}", cmd, e.getMessage());
        }
        return this.root;
    }
}
