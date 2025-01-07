package org.fxf.source.reader;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.jdbc.source.reader.RecordAndOffset;
import org.fxf.source.split.MySourceSplit;
import org.fxf.source.split.MySourceSplitState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class MySourceReader<T, SplitT extends MySourceSplit<?>> extends SingleThreadMultiplexSourceReaderBase<RecordAndOffset<T>, T, SplitT, MySourceSplitState<SplitT>> {
    private static final Logger LOG = LoggerFactory.getLogger(MySourceReader.class);

    public MySourceReader(Configuration config, SourceReaderContext context) {
        super(
                MySourceSplitReader::new,
                new MySourceRecordEmitter<>(),
                config,
                context);
    }

    @Override
    public void start() {
        context.sendSplitRequest();
    }

    @Override
    protected void onSplitFinished(Map<String, MySourceSplitState<SplitT>> map) {

    }

    @Override
    protected MySourceSplitState<SplitT> initializedState(SplitT splitT) {
        return null;
    }

    @Override
    protected SplitT toSplitType(String s, MySourceSplitState<SplitT> splitTMySourceSplitState) {
        return null;
    }
}
