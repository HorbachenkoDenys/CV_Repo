package com.epam.bigData.testMR;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer extends Reducer<ExpediaKey, ExpediaValue, ExpediaKey, ExpediaValue> {
    @Override
    protected void reduce(ExpediaKey key, Iterable<ExpediaValue> values, Context context) throws IOException, InterruptedException {
        for(ExpediaValue value : values) {
            context.write(key, value);
        }
    }
}
