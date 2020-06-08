package com.horbachenkodenis;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;


public class NaturalKeyPartitioner extends Partitioner<CompositeKey, NullWritable> {

    @Override
    public int getPartition(CompositeKey compositeKey, NullWritable values, int numPartitions) {

        return compositeKey.getHotelId().hashCode() % numPartitions;
    }
}

