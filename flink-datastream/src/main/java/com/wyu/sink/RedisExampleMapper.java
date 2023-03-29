package com.wyu.sink;

import com.wyu.transformation.model.Log;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * 官方给的RedisMapper不够灵活
 * 我们可以像MySQL那样extends RichSinkFunction
 * @author novo
 * @since 2023-03-29
 */
public class RedisExampleMapper implements RedisMapper<Log> {


    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.HSET, "flink-example");
    }

    @Override
    public String getKeyFromData(Log log) {
        return log.getDomain();
    }

    @Override
    public String getValueFromData(Log log) {
        return String.valueOf(log.getTraffic());
    }
}
