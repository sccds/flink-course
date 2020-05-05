package com.twq.api.window.time;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.TimeUnit;

public class TestSource implements SourceFunction<String> {

    FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        // 控制大约在10s的倍数时间点发送事件
        String currTime = String.valueOf(System.currentTimeMillis());
        // 截取字符串的后面四位转成 integer
        while (Integer.valueOf(currTime.substring(currTime.length() - 4)) > 100) { // 大约10s无法精确，所以 >100毫秒
            currTime = String.valueOf(System.currentTimeMillis());
            continue;
        }
        System.out.println("开始发送事件的时间: " + dateFormat.format(System.currentTimeMillis()));

        // 第13s发送两个事件
        TimeUnit.SECONDS.sleep(13);
        ctx.collect("a, " + System.currentTimeMillis());
        // ctx.collect("a, " + System.currentTimeMillis());

        // 产生一个事件，但是由于网络问题没有发送，第19s发送
        String event = "a, " + System.currentTimeMillis();

        // 第16s发送一个事件
        TimeUnit.SECONDS.sleep(3);
        ctx.collect("a, " + System.currentTimeMillis());

        // 第19s发送
        TimeUnit.SECONDS.sleep(3);
        ctx.collect(event);

        TimeUnit.SECONDS.sleep(300);
    }

    @Override
    public void cancel() {

    }
}
