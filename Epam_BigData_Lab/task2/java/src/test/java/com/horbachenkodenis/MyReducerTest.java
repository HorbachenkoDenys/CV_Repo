package com.horbachenkodenis;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class MyReducerTest {
    @Test
    public void testReducerCold() throws Exception {
        final NullWritable nullWritable = NullWritable.get();
        final CompositeKey compositeKey = new CompositeKey(15L, "2018-10-21 08:10:37", 5,
                8, 71, 552, 6814, null,
                53, 0, 0, 7, "2019-06-12", "2019-06-14",
                1, 0, 1, 7051, 1,3562841868645L);
        List<NullWritable> list = new ArrayList<NullWritable>();
        list.add(NullWritable.get());
        new ReduceDriver<CompositeKey, NullWritable, CompositeKey, NullWritable>()
                .withReducer(new MyReducer())
                .withInput(compositeKey, list)
                .withOutput(compositeKey, nullWritable)
                .runTest();

    }
}


