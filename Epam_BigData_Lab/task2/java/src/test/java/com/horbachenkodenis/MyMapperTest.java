package com.horbachenkodenis;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

public class MyMapperTest {
    final String input = "15,2015-10-21 08:10:37,5,8,71,522,6814,null,53,0,0,7,2019-06-12,2019-06-14,1,0,1,7051,1,3562841868645";
    MapDriver<Object, Text, CompositeKey, NullWritable> mapDriver;
    private Long id = 15L;
    private String dateTime = "2018-10-21 08:10:37";
    private Integer siteName = 5;
    private Integer posaContinent = 8;
    private Integer userLocationCountry = 71;
    private Integer userLocationRegion = 522;
    private Integer userLocationCity = 6814;
    private Double origDestinationDistance = null;
    private Integer userId = 53;
    private Integer isMobile = 0;
    private Integer isPackage = 0;
    private Integer channel = 7;
    private String srchCI = "2019-06-12";
    private String srchCO = "2019-06-14";
    private Integer srchAdultsCount = 1;
    private Integer srchChildrenCount = 0;
    private Integer srchRmCount = 1;
    private Integer srchDestinationId = 7051;
    private Integer srchDestinationTypeId = 1;
    private Long hotelId = 3562841868645L;


    @Before
    public void setUp() {
        MyMapper mapper = new MyMapper();
        mapDriver = MapDriver.newMapDriver();
        mapDriver.setMapper(mapper);
    }

    @Test
    public void testMapper() throws Exception {
        mapDriver.withInput(new LongWritable(), new Text(input));
        mapDriver.withOutput(new CompositeKey(id, dateTime, siteName, posaContinent, userLocationCountry,
                userLocationRegion, userLocationCity, origDestinationDistance,
                userId, isMobile, isPackage, channel, srchCI, srchCO,
                srchAdultsCount, srchChildrenCount, srchRmCount, srchDestinationId, srchDestinationTypeId,
                hotelId), NullWritable.get());

        mapDriver.runTest();
    }
}

