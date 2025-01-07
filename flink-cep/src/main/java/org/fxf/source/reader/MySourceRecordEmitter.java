package org.fxf.source.reader;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.jdbc.source.reader.RecordAndOffset;
import org.fxf.source.split.MySourceSplit;
import org.fxf.source.split.MySourceSplitState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySourceRecordEmitter<T, SplitT extends MySourceSplit<?>>
        implements RecordEmitter<RecordAndOffset<T>, T, MySourceSplitState<SplitT>> {
    private static final Logger LOG = LoggerFactory.getLogger(MySourceRecordEmitter.class);

    @Override
    public void emitRecord(RecordAndOffset<T> recordAndOffset, SourceOutput<T> output, MySourceSplitState<SplitT> splitState) {
        LOG.info("emitRecord: {}", recordAndOffset.getRecord());
        output.collect(recordAndOffset.getRecord());
    }
}
