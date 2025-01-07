package org.fxf.source.split;

import lombok.Getter;

@Getter
public class MySourceSplitState<SplitT extends MySourceSplit<?>> {
    private final SplitT split;
    private final long index;

    public MySourceSplitState(SplitT split) {
        this.split = split;
        this.index = 0;
    }

}
