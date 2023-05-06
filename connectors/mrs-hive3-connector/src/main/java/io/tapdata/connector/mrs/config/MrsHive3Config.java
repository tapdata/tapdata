package io.tapdata.connector.mrs.config;

import io.tapdata.connector.hive.config.HiveConfig;
import io.tapdata.connector.mrs.util.Krb5Util;
import io.tapdata.kit.EmptyKit;

import java.util.Map;

public class MrsHive3Config extends HiveConfig {

    public MrsHive3Config(String connectorId) {
        super();
        this.connectorId = connectorId;
    }

    public String getDatabaseUrlPattern() {
        // last %s reserved for extend params
        return "jdbc:" + getDbType() + "://%s/%s%s";
    }

    public String getConnectionString() {
        String connectionString = nameSrvAddr + "/" + getDatabase();
        if (EmptyKit.isNotBlank(getSchema())) {
            connectionString += "/" + getSchema();
        }
        return connectionString;
    }

    //deal with extend params no matter there is ?
    public String getDatabaseUrl() {
        if (EmptyKit.isNull(this.getExtParams())) {
            this.setExtParams("");
        }
        if (EmptyKit.isNotEmpty(this.getExtParams()) && !this.getExtParams().startsWith("?") && !this.getExtParams().startsWith(":")) {
            this.setExtParams("?" + this.getExtParams());
        }
        StringBuilder strBuilder = new StringBuilder(String.format(this.getDatabaseUrlPattern(), this.getNameSrvAddr(), this.getDatabase(), this.getExtParams()));
        if (krb5) {
            strBuilder.append(";principal=")
                    .append(principal)
                    .append(";user.principal=")
                    .append(getUser())
                    .append(";user.keytab=")
                    .append(Krb5Util.keytabPath(krb5Path))
                    .append(";");
        }
        return strBuilder.toString();
    }

    @Override
    public MrsHive3Config load(Map<String, Object> map) {
        assert beanUtils != null;
        beanUtils.mapToBean(map, this);
        if (krb5) {
            krb5Path = Krb5Util.saveByCatalog("connections-" + connectorId, krb5Keytab, krb5Conf, true);
        }
        return this;
    }

    private final String connectorId;
    private String nameSrvAddr;
    private Boolean krb5;
    private String krb5Path;
    private String krb5Keytab;
    private String krb5Conf;
    private Boolean ssl;
    private String principal;

    public String getNameSrvAddr() {
        return nameSrvAddr;
    }

    public void setNameSrvAddr(String nameSrvAddr) {
        this.nameSrvAddr = nameSrvAddr;
    }

    public Boolean getKrb5() {
        return krb5;
    }

    public void setKrb5(Boolean krb5) {
        this.krb5 = krb5;
    }

    public String getKrb5Path() {
        return krb5Path;
    }

    public void setKrb5Path(String krb5Path) {
        this.krb5Path = krb5Path;
    }

    public String getKrb5Keytab() {
        return krb5Keytab;
    }

    public void setKrb5Keytab(String krb5Keytab) {
        this.krb5Keytab = krb5Keytab;
    }

    public String getKrb5Conf() {
        return krb5Conf;
    }

    public void setKrb5Conf(String krb5Conf) {
        this.krb5Conf = krb5Conf;
    }

    public Boolean getSsl() {
        return ssl;
    }

    public void setSsl(Boolean ssl) {
        this.ssl = ssl;
    }

    public String getPrincipal() {
        return principal;
    }

    public void setPrincipal(String principal) {
        this.principal = principal;
    }
}
