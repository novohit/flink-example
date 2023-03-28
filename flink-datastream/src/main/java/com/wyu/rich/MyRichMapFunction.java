package com.wyu.rich;

import com.wyu.transformation.model.Log;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;

/**
 * @author novo
 * @since 2023-03-28
 */
@Slf4j
public class MyRichMapFunction extends RichMapFunction<String, Log> {

    /**
     * @param value The input value.
     * @return
     * @throws Exception
     */
    @Override
    public Log map(String value) throws Exception {
        log.error("map method");
        String[] split = value.split(",");
        Log log = Log.builder()
                .time(Long.parseLong(split[0].trim()))
                .domain(split[1].trim())
                .traffic(Double.parseDouble(split[2].trim()))
                .build();
        return log;
    }

    @Override
    public RuntimeContext getRuntimeContext() {
        return super.getRuntimeContext();
    }

    /**
     * 一个并行度执行一次open 如果并行度是8则会执行8次open
     * @param parameters The configuration containing the parameters attached to the contract.
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        log.error("open method");
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        log.error("close method");
        super.close();
    }
}
