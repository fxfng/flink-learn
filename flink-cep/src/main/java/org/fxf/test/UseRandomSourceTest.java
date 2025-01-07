package org.fxf.test;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.fxf.source.RandomSource;

import java.util.Arrays;
import java.util.List;

public class UseRandomSourceTest {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setString("heartbeat.interval", "120000");       // 心跳间隔
        configuration.setString("heartbeat.timeout", "300000");      // 心跳超时
        configuration.setString("taskmanager.registration.timeout", "300000"); // 注册超时
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.createLocalEnvironment(configuration);
        sEnv.setParallelism(3);
        // 根据指定列表，生成数据结果中补充系统时间
        // ["Tim", "Kate", "John"] => {"name":"Tim", "Time":"2024-10-29 10:20:05"}, {"name":"Kate", "Time":"2024-10-29 10:20:03"}
        // 根据并行度和轮次决定每个task中元素的数量
        // 采用轮询方式分别分配给每个并行度
        List<String> initElements = Arrays.asList("Tim", "Kate", "John", "Felix", "Trump");
        DataStreamSource<JSONObject> randomSource = sEnv.fromSource(new RandomSource(initElements, 3), WatermarkStrategy.noWatermarks(), "Random Source");
        randomSource.print();
        sEnv.execute("Random Source");
    }
}
