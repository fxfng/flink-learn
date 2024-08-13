package org.fxf.cep;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.EventComparator;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.fxf.bean.StringBean;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class CepTestDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.getConfig().setAutoWatermarkInterval(100L);
        senv.setParallelism(1);
        DataStreamSource<StringBean> sourceDs = senv.fromElements(
                new StringBean("a1", 1L),
                new StringBean("b1", 2L),
                new StringBean("b2", 3L),
                new StringBean("a2", 4L),
                new StringBean("b3", 5L),
                new StringBean("a3", 6L),
                new StringBean("c", 7L),
                new StringBean("b4", 8L),
                new StringBean("c2", 9L));
        SingleOutputStreamOperator<StringBean> dsWithWt = sourceDs.assignTimestampsAndWatermarks(
                WatermarkStrategy.
                        <StringBean>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((value, timestamp) -> value.getTs())
        );
        // AfterMatchSkipStrategy.skipToFirst("middle")
        Pattern<StringBean, StringBean> pattern = Pattern.<StringBean>begin("start", AfterMatchSkipStrategy.skipToLast("middle"))
                .where(new IterativeCondition<StringBean>() {
                    @Override
                    public boolean filter(StringBean value, Context<StringBean> ctx) {
                        System.out.println("start cep: " + value);
                        return value.getData().startsWith("a");
                    }
                })
                .followedBy("middle")
                .where(new IterativeCondition<StringBean>() {
                    @Override
                    public boolean filter(StringBean value, Context<StringBean> ctx) {
                        System.out.println("middle cep: " + value);
                        return value.getData().startsWith("b");
                    }
                })
                .oneOrMore()
                .followedBy("end")
                .where(SimpleCondition.of(value -> {
                    System.out.println("end cep: " + value);
                    return value.getData().startsWith("c");
                }));
        PatternStream<StringBean> patternDs = CEP.pattern(dsWithWt, pattern, (EventComparator<StringBean>) (o1, o2) -> o1.getTs().compareTo(o2.getTs()));

        patternDs.process(new PatternProcessFunction<StringBean, StringBean>() {
            @Override
            public void processMatch(Map<String, List<StringBean>> match, Context ctx, Collector<StringBean> out) {
                System.out.println(match);
//                out.collect(match.get("start").get(0));
            }
        }).print();

        senv.execute("cep");
    }
}
