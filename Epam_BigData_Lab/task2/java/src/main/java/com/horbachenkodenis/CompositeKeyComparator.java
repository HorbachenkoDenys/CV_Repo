package com.horbachenkodenis;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeKeyComparator extends WritableComparator {
    protected CompositeKeyComparator() {
        super(CompositeKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CompositeKey compositeKeyA = (CompositeKey) a;
        CompositeKey compositeKeyB = (CompositeKey) b;

        int compare = compositeKeyB.getHotelId().compareTo(compositeKeyA.getHotelId());
        if (compare != 0){
            return compare;
        }
        compare = compositeKeyB.getSrchCI().compareTo(compositeKeyA.getSrchCI());
        if (compare != 0){
            return compare;
        }
        compare = compositeKeyB.getId().compareTo(compositeKeyA.getId());
        return compare;
    }
}

