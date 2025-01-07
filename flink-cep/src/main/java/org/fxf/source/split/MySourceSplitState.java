package org.fxf.source.split;

public class MySourceSplitState<SplitT extends MySourceSplit<?>> {
    private final SplitT split;
    private long index;

    public MySourceSplitState(SplitT split) {
        this.split = split;
        this.index = 0;
    }

    public long getIndex() {
        return index;
    }

    public SplitT getSplit() {
        return split;
    }
}
