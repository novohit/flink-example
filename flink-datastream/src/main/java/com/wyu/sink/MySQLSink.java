package com.wyu.sink;

import com.wyu.transformation.model.Log;
import com.wyu.util.MySQLUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * @author novo
 * @since 2023-03-29
 */
public class MySQLSink extends RichSinkFunction<Log> {
    Connection connection;

    PreparedStatement insertPsmt;
    PreparedStatement updatePsmt;

    @Override
    public RuntimeContext getRuntimeContext() {
        return super.getRuntimeContext();
    }

    @Override
    public void invoke(Log value, Context context) throws Exception {
        updatePsmt.setDouble(1, value.getTraffic());
        updatePsmt.setString(2, value.getDomain());
        updatePsmt.execute();
        if (updatePsmt.getUpdateCount() < 1) {
            insertPsmt.setString(1, value.getDomain());
            insertPsmt.setDouble(2, value.getTraffic());
            insertPsmt.execute();
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = MySQLUtils.getConnection();
        insertPsmt = connection.prepareStatement("insert into log (domain, traffic) values (?, ?)");
        updatePsmt = connection.prepareStatement("update log set traffic = ? where domain = ?");
    }

    @Override
    public void close() throws Exception {
        if (insertPsmt != null) {
            insertPsmt.close();
        }
        if (updatePsmt != null) {
            updatePsmt.close();
        }
        MySQLUtils.close(connection);
    }
}
