package com.epam.bigData.testMR;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<Object, Text, ExpediaKey, ExpediaValue> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String text = value.toString();
        String[] lines = text.split("\n");

        ExpediaKey expediaKey = null;
        ExpediaValue expediaValue = null;
        for (String str : lines) {
            String[] element = str.split(",");

            expediaKey = new ExpediaKey();
            expediaValue = new ExpediaValue();

            expediaKey.setId(element[0]);
            expediaKey.setSrchCi(element[12]);
            expediaKey.setHotelId(element[19]);

            expediaValue.setDateTime(element[1]);
            expediaValue.setSiteName(element[2]);
            expediaValue.setPosaContinent(element[3]);
            expediaValue.setUserLocationCountry(element[4]);
            expediaValue.setUserLocationRegion(element[5]);
            expediaValue.setUserLocationCity(element[6]);
            expediaValue.setOrigDestinationDistance(element[7]);
            expediaValue.setUserId(element[8]);
            expediaValue.setIsMobile(element[9]);
            expediaValue.setIsPackage(element[10]);
            expediaValue.setChannel(element[11]);
            expediaValue.setSrchCo(element[13]);
            expediaValue.setSrchAdultsCount(element[14]);
            expediaValue.setSrchChildrenCount(element[15]);
            expediaValue.setSrchRmCount(element[16]);
            expediaValue.setSrchDestinationId(element[17]);
            expediaValue.setSrchDectinationTypeId(element[18]);

            if (expediaValue.getSrchAdultsCount() >= 2) {
                context.write(expediaKey, expediaValue);
            }
        }
    }
}
