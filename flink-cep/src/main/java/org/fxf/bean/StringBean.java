package org.fxf.bean;

public class StringBean {
    private String data;
    private Long ts;

    public StringBean(String data, Long ts) {
        this.data = data;
        this.ts = ts;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "StringBean{" +
                "data='" + data + '\'' +
                ", ts=" + ts +
                '}';
    }
}
