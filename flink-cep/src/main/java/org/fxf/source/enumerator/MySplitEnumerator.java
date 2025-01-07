package org.fxf.source.enumerator;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.fxf.source.split.MySourceSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

public class MySplitEnumerator<SplitT extends MySourceSplit<?>> implements SplitEnumerator<SplitT, Collection<SplitT>> {
    private static final Logger LOG = LoggerFactory.getLogger(MySplitEnumerator.class);
    private final SplitEnumeratorContext<SplitT> context;
    private final Queue<SplitT> remainSplits;

    /**
     *
     * @param context   SplitEnumeratorContext
     * @param splits    待读取的split
     */
    public MySplitEnumerator(SplitEnumeratorContext<SplitT> context, Collection<SplitT> splits) {
        this.context = context;
        this.remainSplits = new ArrayDeque<>(splits);
    }

    // subtask分配split
    @Override
    public void start() {
        LOG.info("start enumerator split size: {}", remainSplits.size());
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        LOG.info("handle split request");
        SplitT nextSplit = remainSplits.poll();
        LOG.info("handle split request next split = {}", nextSplit);
        if (nextSplit != null) {
            context.assignSplit(nextSplit, subtaskId);
        } else {
            context.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public void addSplitsBack(List<SplitT> splits, int subtaskId) {
        this.remainSplits.addAll(splits);
    }

    @Override
    public void addReader(int subtaskId) {
        LOG.info("Reader registered for subtask: {}", subtaskId);
        if (!context.registeredReaders().containsKey(subtaskId)) {
            for (SplitT split : remainSplits) {
                context.assignSplit(split, subtaskId);
            }
        }
    }

    @Override
    public Collection<SplitT> snapshotState(long checkpointId) throws Exception {
        return remainSplits;
    }

    @Override
    public void close() throws IOException {

    }
}
