package com.wyu.source;

import com.wyu.source.model.Student;
import com.wyu.util.MySQLUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * 同理也可以继承RichParallelSourceFunction类实现一个并行的MySQL Source
 *
 * @author novo
 * @since 2023-03-29
 */
@Slf4j
public class MysqlSource extends RichSourceFunction<Student> {

    Connection connection;

    PreparedStatement preparedStatement;

    @Override
    public void run(SourceContext<Student> ctx) throws Exception {
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()) {
            long id = resultSet.getLong("id");
            String username = resultSet.getString("username");
            int age = resultSet.getInt("age");
            Student student = new Student(id, username, age);
            //log.error("stu:{}", student);
            ctx.collect(student);
        }
    }

    @Override
    public void cancel() {

    }


    @Override
    public RuntimeContext getRuntimeContext() {
        return super.getRuntimeContext();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = MySQLUtils.getConnection();
        preparedStatement = connection.prepareStatement("select * from student");
    }

    @Override
    public void close() throws Exception {
        MySQLUtils.close(connection, preparedStatement);
    }
}
