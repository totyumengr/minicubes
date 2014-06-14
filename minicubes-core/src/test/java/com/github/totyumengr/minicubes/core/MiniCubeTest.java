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

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;

import com.github.totyumengr.minicubes.core.MiniCube;
import com.github.totyumengr.minicubes.core.FactTable.FactTableBuilder;

/**
 * @author mengran
 *
 */
public class MiniCubeTest {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(MiniCubeTest.class);
    
    private static MiniCube miniCube;
    
    @BeforeClass
    public static void prepare() throws Throwable {
        
        FactTableBuilder builder = new FactTableBuilder().build("MiniCubeTest")
            .addDimColumns(Arrays.asList(new String[] {"the_date", "tradeId", "productLineId", "postId"}))
            .addIndColumns(Arrays.asList(new String[] {"csm", "cash", "click", "shw"}));
        
        long startTime = System.currentTimeMillis();
        LOGGER.info("prepare - start: {}", startTime);
        ClassPathResource resource = new ClassPathResource("data_fc_bd_qs_day_detail_20140606.data");
        BufferedReader reader = new BufferedReader(new InputStreamReader(resource.getInputStream()));
        String line = null;
        Long index = 0L;
        while ((line = reader.readLine()) != null) {
            String[] split = line.split("\t");
            index++;
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
    public void testSum() {
        
        Assert.assertNotNull(miniCube);
        
        Assert.assertEquals("138240687.91500000", miniCube.sum("csm").toString());
    }
    
}
