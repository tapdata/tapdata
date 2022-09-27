package io.tapdata.zoho.service.commandMode;

import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.CommandInfo;
import io.tapdata.pdk.apis.entity.CommandResult;
import io.tapdata.zoho.entity.CommandResultV2;
import io.tapdata.zoho.enums.FieldModelType;
import io.tapdata.zoho.service.zoho.OrganizationFieldLoader;

import java.util.Map;


//command -> FieldsCommand
public class FieldsCommand extends ConfigContextChecker<Object> implements CommandMode {
    @Override
    public CommandResult command(TapConnectionContext connectionContext, CommandInfo commandInfo) {
        return new CommandResult()
                .result(
                        OrganizationFieldLoader.create(connectionContext)
                                .allOrganizationFields(FieldModelType.TICKETS).getResult()
                );
    }

    @Override
    protected boolean checkerConfig(Map<String, Object> context) {
        return true;
    }

    @Override
    protected CommandResultV2 command(Object entity) {
        return null;
    }
}
