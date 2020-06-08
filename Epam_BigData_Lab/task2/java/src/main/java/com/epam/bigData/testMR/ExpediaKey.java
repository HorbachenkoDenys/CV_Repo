package com.epam.bigData.testMR;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ExpediaKey implements WritableComparable<ExpediaKey> {
    private Long hotelId;
    private Long srchCi;
    private Long id;

    public Long getHotelId() {
        return hotelId;
    }

    public void setHotelId(String hotelId) {
        this.hotelId = Long.parseLong(hotelId);
    }

    public Long getSrchCi() {
        return srchCi;
    }

    public void setSrchCi(String srchCi) {
        this.srchCi = Long.parseLong(srchCi);
    }

    public Long getId() {
        return id;
    }

    public void setId(String id) {
        this.id = Long.parseLong(id);
    }

    @Override
    public String toString() {
        return "ExpediaKey{" +
                "hotelId=" + hotelId +
                ", srchCi=" + srchCi +
                ", id=" + id +
                '}';
    }

    @Override
    public int compareTo(ExpediaKey expediaKey) {
        int comp = hotelId.compareTo(expediaKey.getHotelId());
        if (comp != 0) {
            return comp;
        }
        comp = srchCi.compareTo(expediaKey.getSrchCi());
        if (comp != 0) {
            return comp;
        }
        comp = id.compareTo(expediaKey.getId());
        return comp;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(srchCi);
        dataOutput.writeLong(hotelId);
        dataOutput.writeLong(id);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        srchCi = dataInput.readLong();
        hotelId = dataInput.readLong();
        id = dataInput.readLong();
    }
}
