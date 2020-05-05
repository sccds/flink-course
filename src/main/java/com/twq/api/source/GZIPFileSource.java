package com.twq.api.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.*;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

// 自定义 Flink 的 Source, 从gzip 中读取文件，模拟发送数据
public class GZIPFileSource implements SourceFunction<String> {
    private String dataFilePath;

    public GZIPFileSource(String dataFilePath) {
        this.dataFilePath = dataFilePath;
    }

    private Random random = new Random();

    private InputStream inputStream;
    private BufferedReader reader;

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        // 发送数据
        // 首先要读取 .gz 文件
        inputStream = new GZIPInputStream(new FileInputStream(dataFilePath));
        reader = new BufferedReader(new InputStreamReader(inputStream));

        String line = null;
        while ((line = reader.readLine()) != null) {
            // 随机模拟发送数据
            TimeUnit.MILLISECONDS.sleep(random.nextInt(500));
            // 发送数据
            ctx.collect(line);
        }

        // 文件读完也需要关闭资源
        reader.close();
        reader = null;
        inputStream.close();
        inputStream = null;
    }

    @Override
    public void cancel() {
        // flink job被取消的时候调动方法
        // 关闭资源
        try {
            if (reader != null) {
                reader.close();
            }
            if (inputStream != null) {
                inputStream.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
