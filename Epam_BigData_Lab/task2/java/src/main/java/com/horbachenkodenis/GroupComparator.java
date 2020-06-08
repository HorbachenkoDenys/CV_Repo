package com.horbachenkodenis;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Group all of the records are already in secondary-sort
 * order.
 *
 */
public class GroupComparator extends WritableComparator {
    protected GroupComparator() {
        super(CompositeKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CompositeKey compositeKeyA = (CompositeKey) a;
        CompositeKey compositeKeyB = (CompositeKey) b;

        return compositeKeyA.getHotelId().compareTo(compositeKeyB.getHotelId());
    }
}

