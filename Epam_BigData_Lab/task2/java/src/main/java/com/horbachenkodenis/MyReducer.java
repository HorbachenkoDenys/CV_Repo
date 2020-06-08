package com.horbachenkodenis;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * This reducer will fetch the partitions (from different mappers) and then sort them by ('hotelId', 'srchCI', 'id')
 */
public class MyReducer extends Reducer<CompositeKey, NullWritable, CompositeKey, NullWritable> {
    @Override
    protected void reduce(CompositeKey key, Iterable<NullWritable> nullWriteable, Context context)
            throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}

