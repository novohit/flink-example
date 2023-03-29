package com.wyu.partitioner;

import org.apache.flink.api.common.functions.Partitioner;


import java.util.Map;

/**
 * @author novo
 * @since 2023-03-29
 */
public class CustomPartitioner implements Partitioner<String> {

    /**
     * 自定义分区器，也就是该数据由是哪个并行度输出
     * @param key The key.
     * @param numPartitions The number of partitions to partition into.
     * @return
     */
    @Override
    public int partition(String key, int numPartitions) {
        if (key.equals("a.com")) {
            return 0;
        } else if (key.equals("b.com")) {
            return 1;
        } else {
            return 2;
        }
    }
}
