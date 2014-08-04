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
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.totyumengr.minicubes.core.FactTable.FactTableBuilder;

/**
 * @author mengran
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MiniCubeTest {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(MiniCubeTest.class);
    
    private static MiniCube miniCube;
    
    @BeforeClass
    public static void prepare() throws Throwable {
        
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
            builder.addDimDatas(index, Arrays.asList(new Long[] {
                Long.valueOf(split[0]), Long.valueOf(split[1]), Long.valueOf(split[2]), Long.valueOf(split[3])}));
            builder.addIndDatas(index, Arrays.asList(new BigDecimal[] {
                new BigDecimal(split[4]), new BigDecimal(split[5]), 
                    new BigDecimal(split[6]), new BigDecimal(split[7])}));
        }
        reader.close();
        LOGGER.info("prepare - end: {}, {}ms", System.currentTimeMillis(), System.currentTimeMillis() - startTime);
        
        miniCube = new MiniCube(builder.done());
    }
    
    @Test
    public void test1_1_Sum_20140606() throws Throwable {
        
        Map<String, List<Long>> filter = new HashMap<String, List<Long>>(1);
        filter.put("the_date", Arrays.asList(new Long[] {20140606L}));
        for (int i = 0; i < 3; i++) {
            Assert.assertEquals("138240687.91500000", miniCube.sum("csm", filter).toString());
            Thread.sleep(1000L);
        }
    }
    
    @Test
    public void test1_2_Sum_filter_tradeId() throws Throwable {
        
        Map<String, List<Long>> filter = new HashMap<String, List<Long>>(1);
        filter.put("tradeId", Arrays.asList(new Long[] {
            3205L, 3206L, 3207L, 3208L, 3209L, 3210L, 3212L, 3299L, 
            3204L, 3203L, 3202L, 3201L, 3211L}));
        LOGGER.info(new ObjectMapper().writeValueAsString(filter));
        
        
        for (int i = 0; i < 5; i++) {
            Assert.assertEquals("41612111.56000000", miniCube.sum("csm", filter).toString());
            Thread.sleep(1000L);
        }
        
    }
    
    @Test
    public void test_2_1_Group_tradeId_filter_tradeId() throws Throwable {
        
        Map<String, List<Long>> filter = new HashMap<String, List<Long>>(1);
        filter.put("tradeId", Arrays.asList(new Long[] {
            3205L, 3206L, 3207L, 3208L, 3209L, 3210L, 3212L, 3299L, 
            3204L, 3203L, 3202L, 3201L, 3211L}));
        
        for (int i = 0; i < 5; i++) {
            Map<Long, BigDecimal> group = miniCube.sum("csm", "tradeId", filter);
            Assert.assertEquals("543138.14000000", group.get(3201L).toString());
            Assert.assertEquals("8994005.53000000", group.get(3202L).toString());
            Assert.assertEquals("7236913.98000000", group.get(3203L).toString());
            Assert.assertEquals("4711386.93000000", group.get(3299L).toString());
            Assert.assertEquals("5060157.38000000", group.get(3204L).toString());
            Assert.assertEquals("1878194.96000000", group.get(3205L).toString());
            Assert.assertEquals("2366059.53000000", group.get(3206L).toString());
            Assert.assertEquals("2831228.01000000", group.get(3207L).toString());
            Assert.assertEquals("302801.76000000", group.get(3208L).toString());
            Assert.assertEquals("759776.22000000", group.get(3209L).toString());
            Assert.assertEquals("6359417.21000000", group.get(3210L).toString());
            Assert.assertEquals("355185.79000000", group.get(3211L).toString());
            Assert.assertEquals("213846.12000000", group.get(3212L).toString());
            Thread.sleep(1000L);
        }
    }
    
    @Test
    public void test_2_2_Group_tradeId() throws Throwable {
        
        for (int i = 0; i < 5; i++) {
            Map<Long, BigDecimal> group = miniCube.sum("csm", "tradeId", null);
            Assert.assertEquals(210, group.size());
            Assert.assertEquals("274795.77600000", group.get(-1L).toString());
            Assert.assertEquals("108080.82000000", group.get(3099L).toString());
            Assert.assertEquals("72360.92000000", group.get(3004L).toString());
            Assert.assertEquals("31828.81000000", group.get(502L).toString());
            Assert.assertEquals("50708.85000000", group.get(505L).toString());
            Thread.sleep(1000L);
        }
    }
    
    @Test
    public void test_2_3_Zero_BigDecimail() throws Throwable {
        
        BigDecimal zero = new BigDecimal(0).setScale(8, BigDecimal.ROUND_HALF_UP);
        System.out.println(zero);
    }
    
}
