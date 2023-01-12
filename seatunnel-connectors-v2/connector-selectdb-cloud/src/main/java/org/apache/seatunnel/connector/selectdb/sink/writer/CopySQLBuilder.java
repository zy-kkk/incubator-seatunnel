package org.apache.seatunnel.connector.selectdb.sink.writer;


import org.apache.seatunnel.connector.selectdb.config.SelectDBConfig;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringJoiner;

public class CopySQLBuilder {
    private final static String COPY_SYNC = "copy.async";
    private final static String COPY_DELETE = "copy.use_delete_sign";
    private final SelectDBConfig selectdbConfig;
    private final List<String> fileList;
    private Properties properties;

    public CopySQLBuilder(SelectDBConfig selectdbConfig, List<String> fileList) {
        this.selectdbConfig = selectdbConfig;
        this.fileList = fileList;
        this.properties = selectdbConfig.getStreamLoadProps();
    }

    public String buildCopySQL() {
        StringBuilder sb = new StringBuilder();
        sb.append("COPY INTO ")
                .append(selectdbConfig.getTableIdentifier())
                .append(" FROM @~('{").append(String.join(",", fileList)).append("}') ")
                .append("PROPERTIES (");

        //copy into must be sync
        properties.put(COPY_SYNC, false);
        if (selectdbConfig.getEnableDelete()) {
            properties.put(COPY_DELETE, true);
        }
        StringJoiner props = new StringJoiner(",");
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            String key = String.valueOf(entry.getKey());
            String value = String.valueOf(entry.getValue());
            String prop = String.format("'%s'='%s'", key, value);
            props.add(prop);
        }
        sb.append(props).append(")");
        return sb.toString();
    }
}
