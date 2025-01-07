package org.fxf.source;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.fxf.source.enumerator.MySplitEnumerator;
import org.fxf.source.reader.MySourceReader;
import org.fxf.source.serializer.MyVersionSerializer;
import org.fxf.source.split.MySourceSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class RandomSource<SplitT extends MySourceSplit<?>> implements Source<JSONObject, SplitT, Collection<SplitT>> {
    private static final Logger LOG = LoggerFactory.getLogger(RandomSource.class);
    private final List<String> elements;
    private final int round;

    /**
     * <h2>元素定义</h2>
     *
     * @param elements 初始元素
     * @param round    轮次，遍历次数
     */
    public RandomSource(List<String> elements, int round) {
        this.elements = elements;
        this.round = round;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    // source先创建enumerator，然后创建reader
    @Override
    public SplitEnumerator<SplitT, Collection<SplitT>> createEnumerator(SplitEnumeratorContext<SplitT> enumContext) throws Exception {

        LOG.info("create enumerator");
        List<SplitT> splits = splitElements(elements, round, enumContext.currentParallelism());
        return new MySplitEnumerator<>(enumContext, splits);
    }

    @Override
    public SplitEnumerator<SplitT, Collection<SplitT>> restoreEnumerator(SplitEnumeratorContext<SplitT> enumContext,
                                                                                       Collection<SplitT> checkpoint) throws Exception {
        return new MySplitEnumerator<>(enumContext, checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<SplitT> getSplitSerializer() {
        return new MyVersionSerializer();
    }

    @Override
    public SimpleVersionedSerializer<Collection<SplitT>> getEnumeratorCheckpointSerializer() {
        return null;
    }

    @Override
    public SourceReader<JSONObject, SplitT> createReader(SourceReaderContext readerContext) throws Exception {
        LOG.info("create reader");
        return new MySourceReader<>(readerContext.getConfiguration(), readerContext);
    }

    // 拆分List，构建Split
    private List<SplitT> splitElements(List<String> elements, int round, int taskNumber) {
        List<SplitT> splits = new ArrayList<>();
        int splitId = 1;

        int n = elements.size();
        for (int i = 0; i < taskNumber; i++) {
            List<String> tmpElements = new ArrayList<>(round);
            for (int j = 0; j < round; j++) {
                tmpElements.add(elements.get((i + j * taskNumber) % n));
            }
            splits.add((SplitT) new MySourceSplit<>(String.valueOf(splitId++), new ArrayList<>(tmpElements)));
            tmpElements.clear();
        }
        return splits;
    }

}
