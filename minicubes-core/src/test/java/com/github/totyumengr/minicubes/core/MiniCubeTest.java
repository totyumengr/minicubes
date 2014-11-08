/*
 * Copyright 2014 Ran Meng
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.totyumengr.minicubes.core;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import md.math.DoubleDouble;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.util.StopWatch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.totyumengr.minicubes.core.FactTable.FactTableBuilder;
import com.github.totyumengr.minicubes.core.FactTable.FactTableBuilderUserDefineDimProvider;

/**
 * @author mengran
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MiniCubeTest {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(MiniCubeTest.class);
    
    private static MiniCube miniCube;
    
    public static class UserDefineDimProvider implements FactTableBuilderUserDefineDimProvider {

        @Override
        public int getOrder() {
            return Integer.MAX_VALUE;
        }

        @Override
        public LinkedHashMap<String, String> getUserDefineDimConfig() {
            
            LinkedHashMap<String, String> map = new LinkedHashMap<String, String>(2);
            map.put("shw_segment", "function shw_segment() {"
                    + " var i_shw = new Number(shw);"
                    + " if (i_shw === null) {return 0;}"
                    + " else if (i_shw < 1000) { return 1;}"
                    + " else if (i_shw >= 1000 && i_shw < 10000) { return 2;}"
                    + " else if (i_shw >= 10000 && i_shw < 100000) { return 3;}"
                    + " else if (i_shw >= 100000 && i_shw < 1000000) { return 4;}"
                    + " else if (i_shw >= 1000000) {return 5;}"
                    + "}");
            map.put("cash_or_not", "function cash_or_not() {"
                    + " var i_cash = new Number(cash);"
                    + " if (i_cash === null || i_cash <= 0) { return 0;}"
                    + " if (i_cash > 0) { return 1;}"
                    + "}");
            return map;
        }
        
    }
    
    @BeforeClass
    public static void prepare() throws Throwable {
        
        Assert.assertTrue(new DoubleDouble(1926).doubleValue() == 1926.0);
        Assert.assertTrue(new DoubleDouble("1926").doubleValue() == 0.1926);
        Assert.assertTrue(new DoubleDouble("1926.000").doubleValue() == 1926.0);
        
        String dataFile = System.getProperty("dataFile", "data_fc_bd_qs_day_detail_20140606.data");
        
        FactTableBuilder builder = new FactTableBuilder().build("MiniCubeTest")
            .addDimColumns(Arrays.asList(new String[] {"the_date", "tradeId", "productLineId", "postId"}))
            .addIndColumns(Arrays.asList(new String[] {"csm", "cash", "click", "shw"}));
        
        long startTime = System.currentTimeMillis();
        LOGGER.info("prepare {} - start: {}", dataFile, startTime);
        ClassPathResource resource = new ClassPathResource(dataFile);
        BufferedReader reader = new BufferedReader(new InputStreamReader(resource.getInputStream()));
        String line = null;
        Integer index = 0;
        while ((line = reader.readLine()) != null) {
            String[] split = line.split("\t");
            index++;
            if (index % 100000 == 0) {
                LOGGER.debug("Load {} records", index);
            }
            builder.addDimDatas(index, Arrays.asList(new Integer[] {
                Integer.valueOf(split[0]), Integer.valueOf(split[1]), Integer.valueOf(split[2]), Integer.valueOf(split[3])}));
            builder.addIndDatas(index, Arrays.asList(new DoubleDouble[] {
                new DoubleDouble(split[4]), new DoubleDouble(split[5]), 
                    new DoubleDouble(split[6] + ".00000000"), new DoubleDouble(split[7] + ".00000000")}));
        }
        reader.close();
        LOGGER.info("prepare - end: {}, {}ms", System.currentTimeMillis(), System.currentTimeMillis() - startTime);
        
        miniCube = new MiniCube(builder.done());
    }
    
    @Test
    public void test_0_1_UserDefineDims_shw_segment() throws Throwable {
        
        Map<String, List<Integer>> filter = new HashMap<String, List<Integer>>(1);
        filter.put("shw_segment", Arrays.asList(new Integer[] {0, 1, 2, 3}));
        miniCube.setParallelMode(true);
        for (int i = 0; i < 5; i++) {
            Assert.assertEquals("3336028826.00000000", miniCube.sum("shw", filter).toString());
            Thread.sleep(1000L);
        }
    }
    
    @Test
    public void test_1_1_Sum_20140606() throws Throwable {
        
        Map<String, List<Integer>> filter = new HashMap<String, List<Integer>>(1);
        filter.put("the_date", Arrays.asList(new Integer[] {20140606}));
        miniCube.setParallelMode(false);
        for (int i = 0; i < 5; i++) {
            Assert.assertEquals("138240687.91500000", miniCube.sum("csm", filter).toString());
            Thread.sleep(1000L);
        }
        miniCube.setParallelMode(true);
        for (int i = 0; i < 5; i++) {
            Assert.assertEquals("138240687.91500000", miniCube.sum("csm", filter).toString());
            Thread.sleep(1000L);
        }
    }
    
    @Test
    public void test_1_2_Sum_filter_tradeId() throws Throwable {
        
        Map<String, List<Integer>> filter = new HashMap<String, List<Integer>>(1);
        filter.put("tradeId", Arrays.asList(new Integer[] {
            3205, 3206, 3207, 3208, 3209, 3210, 3212, 3299, 
            3204, 3203, 3202, 3201, 3211}));
        LOGGER.info(new ObjectMapper().writeValueAsString(filter));
        
        
        for (int i = 0; i < 5; i++) {
            Assert.assertEquals("41612111.56000000", miniCube.sum("csm", filter).toString());
            Thread.sleep(1000L);
        }
        
    }
    
    @Test
    public void test_2_1_Group_tradeId_filter_tradeId() throws Throwable {
        
        Map<String, List<Integer>> filter = new HashMap<String, List<Integer>>(1);
        filter.put("tradeId", Arrays.asList(new Integer[] {
            3205, 3206, 3207, 3208, 3209, 3210, 3212, 3299, 
            3204, 3203, 3202, 3201, 3211}));
        
        for (int i = 0; i < 5; i++) {
            Map<Integer, BigDecimal> group = miniCube.sum("csm", "tradeId", filter);
            Assert.assertEquals("543138.14000000", group.get(3201).toString());
            Assert.assertEquals("8994005.53000000", group.get(3202).toString());
            Assert.assertEquals("7236913.98000000", group.get(3203).toString());
            Assert.assertEquals("4711386.93000000", group.get(3299).toString());
            Assert.assertEquals("5060157.38000000", group.get(3204).toString());
            Assert.assertEquals("1878194.96000000", group.get(3205).toString());
            Assert.assertEquals("2366059.53000000", group.get(3206).toString());
            Assert.assertEquals("2831228.01000000", group.get(3207).toString());
            Assert.assertEquals("302801.76000000", group.get(3208).toString());
            Assert.assertEquals("759776.22000000", group.get(3209).toString());
            Assert.assertEquals("6359417.21000000", group.get(3210).toString());
            Assert.assertEquals("355185.79000000", group.get(3211).toString());
            Assert.assertEquals("213846.12000000", group.get(3212).toString());
            Thread.sleep(1000L);
        }
    }
    
    @Test
    public void test_2_2_Group_tradeId() throws Throwable {
        
        miniCube.setParallelMode(true);
        for (int i = 0; i < 5; i++) {
            Map<Integer, BigDecimal> group = miniCube.sum("csm", "tradeId", null);
            Assert.assertEquals(210, group.size());
            Assert.assertEquals("274795.77600000", group.get(-1).toString());
            Assert.assertEquals("108080.82000000", group.get(3099).toString());
            Assert.assertEquals("72360.92000000", group.get(3004).toString());
            Assert.assertEquals("31828.81000000", group.get(502).toString());
            Assert.assertEquals("50708.85000000", group.get(505).toString());
            Thread.sleep(1000L);
        }
        miniCube.setParallelMode(false);
        for (int i = 0; i < 5; i++) {
            Map<Integer, BigDecimal> group = miniCube.sum("csm", "tradeId", null);
            Assert.assertEquals(210, group.size());
            Assert.assertEquals("274795.77600000", group.get(-1).toString());
            Assert.assertEquals("108080.82000000", group.get(3099).toString());
            Assert.assertEquals("72360.92000000", group.get(3004).toString());
            Assert.assertEquals("31828.81000000", group.get(502).toString());
            Assert.assertEquals("50708.85000000", group.get(505).toString());
            Thread.sleep(1000L);
        }
        miniCube.setParallelMode(true);
    }
    
    @Test
    public void test_2_3_Zero_BigDecimail() throws Throwable {
        
        BigDecimal zero = new BigDecimal(0).setScale(8, BigDecimal.ROUND_HALF_UP);
        System.out.println(zero);
    }
    
    @Test
    public void test_3_1_BitmapIndex_Sum_20140606() throws Throwable {
        
        Map<String, List<Integer>> filter = new HashMap<String, List<Integer>>(1);
        filter.put("the_date", Arrays.asList(new Integer[] {20140606}));
        for (int i = 0; i < 3; i++) {
            Assert.assertEquals("138240687.91500000", miniCube.sum("csm", filter).toString());
            Thread.sleep(1000L);
        }
    }
    
    @Test
    public void test_3_2_BitmapIndex_Sum_filter_tradeId() throws Throwable {
        
        Map<String, List<Integer>> filter = new HashMap<String, List<Integer>>(1);
        filter.put("tradeId", Arrays.asList(new Integer[] {
            3205, 3206, 3207, 3208, 3209, 3210, 3212, 3299, 
            3204, 3203, 3202, 3201, 3211}));
        LOGGER.info(new ObjectMapper().writeValueAsString(filter));
        
        
        for (int i = 0; i < 5; i++) {
            Assert.assertEquals("41612111.56000000", miniCube.sum("csm", filter).toString());
            Thread.sleep(1000L);
        }
        
    }
    
    @Test
    public void test_4_1_DoubleDouble_Sum_20140606() throws Throwable {
        
        Assert.assertEquals("138240687.91500000", miniCube.sum("csm").toString());
    }
    
    @Test
    public void test_5_1_Distinct_20140606() throws Throwable {
        
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        Map<String, List<Integer>> filter = new HashMap<String, List<Integer>>(1);
        Map<Integer, RoaringBitmap> distinct = miniCube.distinct("postId", true, "tradeId", filter);
        stopWatch.stop();
        
        Assert.assertEquals(210, distinct.size());
        Assert.assertEquals(3089, distinct.get(1601).getCardinality());
        Assert.assertEquals(1825, distinct.get(1702).getCardinality());
        Assert.assertEquals(2058, distinct.get(-2).getCardinality());
        
        LOGGER.info(stopWatch.getTotalTimeSeconds() + " used for distinct result {}", distinct.toString());
    }
    
    @Test
    public void test_5_2_DistinctCount_20140606() throws Throwable {
        
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        Map<String, List<Integer>> filter = new HashMap<String, List<Integer>>(1);
        filter.put("tradeId", Arrays.asList(new Integer[] {
            3205, 3206, 3207, 3208, 3209, 3210, 3212, 3299, 
            3204, 3203, 3202, 3201, 3211}));
        Map<Integer, RoaringBitmap> distinct = miniCube.distinct("postId", true, "tradeId", filter);
        stopWatch.stop();
        
        Assert.assertEquals(13, distinct.size());
        Assert.assertEquals(277, distinct.get(3209).getCardinality());
        Assert.assertEquals(186, distinct.get(3211).getCardinality());
        Assert.assertEquals(464, distinct.get(3206).getCardinality());
        LOGGER.info(stopWatch.getTotalTimeSeconds() + " used for distinct result {}", distinct.toString());
        
    }
    
    @Test
    public void test_6_1_merge() {
        
        FactTableBuilder builder = new FactTableBuilder().build("MiniCubeTest-merge")
                .addDimColumns(Arrays.asList(new String[] {"the_date", "tradeId", "productLineId", "postId"}))
                .addIndColumns(Arrays.asList(new String[] {"csm", "cash", "click", "shw"}));
        
        builder.addDimDatas(Integer.MAX_VALUE, Arrays.asList(new Integer[] {20140607, 1, 1, 1}));
        builder.addIndDatas(Integer.MAX_VALUE, Arrays.asList(new DoubleDouble[] {
                new DoubleDouble(123.123), new DoubleDouble(124.124), new DoubleDouble(123), new DoubleDouble(124)}));
        FactTable merge = builder.done();
        // Do merge
        BigDecimal original = miniCube.sum("csm");
        miniCube.merge(new MiniCube(merge));
        
        Assert.assertEquals(original.add(new BigDecimal(123.123).setScale(
                Aggregations.IND_SCALE, BigDecimal.ROUND_HALF_UP)), miniCube.sum("csm"));
        
        Map<String, List<Integer>> filter = new HashMap<String, List<Integer>>(1);
        filter.put("the_date", Arrays.asList(new Integer[] {20140606}));
        miniCube.setParallelMode(false);
        Assert.assertEquals("138240687.91500000", miniCube.sum("csm", filter).toString());
    }
    
}
