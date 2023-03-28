package com.wyu.transformation;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

/**
 * @author novo
 * @since 2023-03-28
 */
public class TransformationApp {
    public static void main(String[] args) throws Exception {
        // 1. 创建上下文
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> source = env.readTextFile("data/access.log");
        source.print();
        // 4. sink
    }

    public static void test() {

    }
}
