package org.fxf.source.split;

import lombok.Getter;
import org.apache.flink.api.connector.source.SourceSplit;

import java.io.Serializable;
import java.util.List;

/**
 * @author fxf
 */
public class MySourceSplit<T> implements SourceSplit, Serializable {
    // 分片编号
    private final String splitId;
    // 分片数据
    @Getter
    private final List<T> elements;

    /**
     * @param splitId  分片编号
     * @param elements 分片数据
     */
    public MySourceSplit(String splitId, List<T> elements) {
        this.splitId = splitId;
        this.elements = elements;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    @Override
    public String toString() {
        return "MySourceSplit{" +
                "splitId='" + splitId + '\'' +
                ", element size =" + elements.size() +
                '}';
    }
}
