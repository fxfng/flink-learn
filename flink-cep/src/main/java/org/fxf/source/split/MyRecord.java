package org.fxf.source.split;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.jdbc.source.reader.RecordAndOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class MyRecord<T> implements RecordsWithSplitIds<RecordAndOffset<T>> {
    private static final Logger LOG = LoggerFactory.getLogger(MyRecord.class.getName());
    private String splitId;
    private String nextSplit = null;
    private List<T> recordsForCurrentSplit;
    private final List<T> recordsForSplit;
    private final Set<String> finishedSplits;

    public MyRecord(String splitId, List<T> recordsForSplit, Set<String> finishedSplits) {
        this.splitId = splitId;
        this.recordsForSplit = recordsForSplit;
        this.finishedSplits = finishedSplits;
    }

    @Nullable
    @Override
    public String nextSplit() {
        this.nextSplit = this.splitId;
        this.splitId = null;

        this.recordsForCurrentSplit = this.nextSplit == null ? null : this.recordsForSplit;
        LOG.info("nextSplit = {}, recordsForCurrentSplit = {}", nextSplit, this.recordsForCurrentSplit);
        return nextSplit;
    }

    @Nullable
    @Override
    public RecordAndOffset<T> nextRecordFromSplit() {
        LOG.debug("execute get nextRecordFromSplit {}", this.recordsForCurrentSplit);
        try {
            if (this.recordsForCurrentSplit != null && !this.recordsForCurrentSplit.isEmpty()) {
                T record = this.recordsForCurrentSplit.remove(0);
                LOG.info("splitId {} nextRecordFromSplit output {}", this.nextSplit, record);
                return new RecordAndOffset<>(record, 0, 0);
            } else {
                LOG.warn("splitId = {}, recordsForCurrentSplit = {}", this.splitId, this.recordsForCurrentSplit);
                return null;
//                LOG.warn("splitId = {}, recordsForCurrentSplit = {}", this.splitId, this.recordsForCurrentSplit);
//                throw new IllegalStateException("No more records for split " + this.splitId);
            }
        } catch (Exception e) {
            LOG.error("nextRecordFromSplit error {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @Override
    public Set<String> finishedSplits() {
        return this.finishedSplits;
    }

    // ----------------------------------------------------------------------
    public static <T> MyRecord<T> finished(String splitId) {
        return new MyRecord<>(null, null, Collections.singleton(splitId));
    }

    public static <T> MyRecord<T> of(String splitId, List<T> records) {
        return new MyRecord<>(splitId, records, Collections.emptySet());
    }


}
