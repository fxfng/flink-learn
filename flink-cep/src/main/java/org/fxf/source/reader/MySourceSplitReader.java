package org.fxf.source.reader;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsRemoval;
import org.apache.flink.connector.jdbc.source.reader.RecordAndOffset;
import org.eclipse.jetty.util.ArrayQueue;
import org.fxf.source.split.MyRecord;
import org.fxf.source.split.MySourceSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class MySourceSplitReader<T, SplitT extends MySourceSplit<?>> implements SplitReader<RecordAndOffset<T>, SplitT> {
    private static final Logger LOG = LoggerFactory.getLogger(MySourceSplitReader.class);
    private final Queue<SplitT> splits;
    private String currentSplitId;

    public MySourceSplitReader() {
        this.splits = new ArrayQueue<>();
    }

    @Override
    public RecordsWithSplitIds<RecordAndOffset<T>> fetch() {
        if (splits.isEmpty()) {
            return finishSplit();
        }
        LOG.info("fetch split size = {}", splits.size());
        final SplitT splitT = splits.poll();
        if (splitT == null) {
            return finishSplit();
        } else {
            currentSplitId = splitT.splitId();
            return MyRecord.of(splitT.splitId(), (List<T>) splitT.getElements());
        }
    }

    @Override
    public void handleSplitsChanges(SplitsChange<SplitT> splitsChange) {
        if (splitsChange instanceof SplitsAddition) {
            splits.addAll(splitsChange.splits());
            LOG.info("Added splits: {}", splitsChange.splits());
        } else if (splitsChange instanceof SplitsRemoval) {
            splits.removeAll(splitsChange.splits());
            LOG.info("Removed splits: {}", splitsChange.splits());
        } else {
            LOG.error("handle splits changes");
        }
    }

    @Override
    public void wakeUp() {

    }

    @Override
    public void close() {

    }

    // -----------------------------------------------------------
    private MyRecord<T> finishSplit() {
        LOG.info("execute finish split {}", currentSplitId);
        MyRecord<T> record = MyRecord.finished(currentSplitId);
        currentSplitId = null;
        return record;
    }
}
