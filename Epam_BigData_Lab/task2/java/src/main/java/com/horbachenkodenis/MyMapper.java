package com.horbachenkodenis;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * This mapper outputs a (CompositeKey, NullWritable) pair for each row.
 */
public class MyMapper extends Mapper<Object, Text, CompositeKey, NullWritable> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String text = value.toString();
        List<String> lines = Arrays.asList(text.split("\n"));


        CompositeKey compositeKey = new CompositeKey();
        for (String strElement : lines) {
            String[] element = strElement.split(",");

            compositeKey.setId(Long.parseLong(element[0]));
            compositeKey.setDateTime(element[1]);
            compositeKey.setSiteName(Integer.parseInt(element[2]));
            compositeKey.setPosaContinent(Integer.parseInt(element[3]));
            compositeKey.setUserLocationCountry(Integer.parseInt(element[4]));
            compositeKey.setUserLocationRegion(Integer.parseInt(element[5]));
            compositeKey.setUserLocationCity(Integer.parseInt(element[6]));
            compositeKey.setOrigDestinationDistance(("null".equals(element[7])) ? null : Double.parseDouble(element[7]));
            compositeKey.setUserId(Integer.parseInt(element[8]));
            compositeKey.setMobile(Integer.parseInt(element[9]));
            compositeKey.setPackage(Integer.parseInt(element[10]));
            compositeKey.setChannel(Integer.parseInt(element[11]));
            compositeKey.setSrchCI(element[12]);
            compositeKey.setSrchCO(element[13]);
            compositeKey.setSrchAdultsCount(Integer.parseInt(element[14]));
            compositeKey.setSrchChildrenCount(Integer.parseInt(element[15]));
            compositeKey.setSrchRmCount(Integer.parseInt(element[16]));
            compositeKey.setSrchDestinationId(Integer.parseInt(element[17]));
            compositeKey.setSrchDestinationTypeId(Integer.parseInt(element[18]));
            compositeKey.setHotelId(Long.parseLong(element[19]));

            if (compositeKey.getSrchAdultsCount() >= 2) {
                context.write(compositeKey, NullWritable.get());
            }
        }
    }
}

