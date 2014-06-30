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
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.totyumengr.minicubes.core.FactTable.FactTableBuilder;

/**
 * 
 * @author mengran
 *
 */
@Controller
public class BootPerformanceController {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(BootPerformanceController.class);
    
    private ObjectMapper objectMapper = new ObjectMapper();
    
    private MiniCube miniCube;
    
    @RequestMapping(value="/init/{cubeName}", method=RequestMethod.POST)
    public @ResponseBody String init(@PathVariable String cubeName, @RequestParam String dataFile) throws Throwable {
        
        FactTableBuilder builder = new FactTableBuilder().build(cubeName)
            .addDimColumns(Arrays.asList(new String[] {"the_date", "tradeId", "productLineId", "postId"}))
            .addIndColumns(Arrays.asList(new String[] {"csm", "cash", "click", "shw"}));
        
        long startTime = System.currentTimeMillis();
        LOGGER.info("prepare {} - start: {}", dataFile, startTime);
        Resource resource = new FileSystemResource(dataFile);
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
        
        LOGGER.info("Forkjoin pool size {}", Runtime.getRuntime().availableProcessors());
        
        miniCube = new MiniCube(builder.done());
        return cubeName + " OK.";
    }
    
    @RequestMapping(value="/sum/{cubeName}/{indName}", method=RequestMethod.GET)
    public @ResponseBody String sum(@PathVariable String cubeName, @PathVariable String indName) {
        
        LOGGER.info("Sum {} of {}", indName, cubeName);
        BigDecimal sum = miniCube.sum(indName);
        return sum.toString();
    }
    
    @RequestMapping(value="/group/{cubeName}/{indName}/by/{groupByDimName}", method={RequestMethod.GET, RequestMethod.POST})
    public @ResponseBody Map<Long, BigDecimal> group(@PathVariable String cubeName, @PathVariable String indName, 
        @PathVariable String groupByDimName, @RequestParam String filterJson) throws Throwable {
        
        LOGGER.info("Group {} of {} by {} with filter {}", indName, cubeName, groupByDimName, filterJson);
        Map<String, List<Long>> filterDims = objectMapper.readValue(filterJson, new TypeReference<Map<String, List<Long>>>() {});
        LOGGER.info("Parse json filter to {}", filterDims);
        
        Map<Long, BigDecimal> group = miniCube.group(indName, groupByDimName, filterDims);
        return group;
    }
}
