package com.wyu.source;

import com.wyu.transformation.model.Log;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * @author novo
 * @since 2023-03-28
 */
public class LogParallelSource implements ParallelSourceFunction<Log> {
    boolean isRunning = true;

    @Override
    public void run(SourceContext<Log> ctx) throws Exception {
        List<String> domains = Arrays.asList("a.com", "b.com", "c.com");
        Random random = new Random();
        while (isRunning) {
            for (int i = 0; i < 5; i++) {
                Log log = Log.builder()
                        .time(2020010102L)
                        .domain(domains.get(random.nextInt(domains.size())))
                        .traffic(i * 100D).build();
                ctx.collect(log);
            }
            Thread.sleep(5000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
