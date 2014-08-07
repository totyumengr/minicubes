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
package com.github.totyumengr.minicubes.cluster;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.hibernate.validator.constraints.NotBlank;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.util.ObjectUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author mengran
 *
 */
@Controller
public class BootTimeSeriesMiniCubeController {

    private static final Logger LOGGER = LoggerFactory.getLogger(BootTimeSeriesMiniCubeController.class);
    
    private ObjectMapper objectMapper = new ObjectMapper();
    
    @Autowired
    private TimeSeriesMiniCubeManager manager;
    
    @RequestMapping(value="/status", method=RequestMethod.GET)
    public @ResponseBody Map<String, List<String>> status() {
        
        Map<String, List<String>> status = new LinkedHashMap<>();
        Collection<String> allCubeIds = manager.allCubeIds();
        status.put("working", allCubeIds.stream().filter(e -> !e.startsWith("?")).collect(Collectors.toList()));
        status.put("awaiting", allCubeIds.stream().filter(e -> e.startsWith("?")).collect(Collectors.toList()));
        
        LOGGER.info("All miniCube size in cluster: {}, working/awaiting is {}/{}", 
                allCubeIds.size(), status.get("working").size(), status.get("awaiting").size());
        
        return status;
    }
    
    @RequestMapping(value="/reassign", method=RequestMethod.POST)
    public @ResponseBody String reassign(@NotBlank @RequestParam String cubeId, 
            @NotBlank @RequestParam String timeSeries) {
        
        LOGGER.info("Try to assign cubeId{} to handle{} request.", cubeId, timeSeries);
        String newCubeId = manager.reassignRole(cubeId, timeSeries);
        LOGGER.info("Sucess to assign cubeId{} to handle{} request.", newCubeId, timeSeries);
        
        return newCubeId;
    }
    
    @RequestMapping(value="/sum", method={RequestMethod.POST, RequestMethod.GET})
    public @ResponseBody BigDecimal sum(@NotBlank @RequestParam String indName, 
            @RequestParam(required=false) String filterDims,
            @NotBlank @RequestParam String... timeSeries) throws Throwable {
        
        LOGGER.info("Try to sum {} on {} with filter {}.", indName, ObjectUtils.getDisplayString(timeSeries), filterDims);
        Map<String, List<Long>> filter = (filterDims == null || "".equals(filterDims)) ? null
                : objectMapper.readValue(filterDims, new TypeReference<Map<String, List<Long>>>() {});
        BigDecimal sum = manager.aggs(timeSeries).sum(indName, filter);
        LOGGER.info("Sucess to sum {} on {} result is {}.", indName, timeSeries, sum);
        
        return sum;
    }
    
    @RequestMapping(value="/groupsum", method={RequestMethod.POST, RequestMethod.GET})
    public @ResponseBody Map<Long, BigDecimal> groupsum(@NotBlank @RequestParam String indName, 
            @RequestParam(required=false) String filterDims,
            @RequestParam(required=false) String groupbyDim,
            @NotBlank @RequestParam String... timeSeries) throws Throwable {
        
        LOGGER.info("Try to sum {} on {} with filter {}.", indName, ObjectUtils.getDisplayString(timeSeries), filterDims);
        Map<String, List<Long>> filter = (filterDims == null || "".equals(filterDims)) ? null
                : objectMapper.readValue(filterDims, new TypeReference<Map<String, List<Long>>>() {});
        Map<Long, BigDecimal> sum = manager.aggs(timeSeries).sum(indName, groupbyDim, filter);
        LOGGER.info("Sucess to sum {} on {} result is {}.", indName, timeSeries, sum);
        
        return sum;
    }
}
