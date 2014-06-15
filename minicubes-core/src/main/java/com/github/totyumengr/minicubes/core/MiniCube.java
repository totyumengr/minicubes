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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.totyumengr.minicubes.core.FactTable.Record;

/**
 * In-memory cube base on java8 stream feature.
 * 
 * @author mengran
 *
 */
public class MiniCube {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(MiniCube.class);
    
    public static final int IND_SCALE = 8;
    public static final long DUMMY_FILTER_DIM = -999999999L;
    
    private FactTable factTable;

    // FIXME: Add dimension table
    public MiniCube(FactTable factTable) {
        super();
        this.factTable = factTable;
    }
    
    private Stream<Record> filter(String indName, Map<String, List<Long>> filterDims) {
        
        if (filterDims == null) {
            filterDims = new HashMap<String, List<Long>>(0);
        }
        List<Predicate<Record>> filters = new ArrayList<Predicate<Record>>(filterDims.size());
        for (Entry<String, List<Long>> entry : filterDims.entrySet()) {
            String key = entry.getKey();
            List<Long> value = entry.getValue();
            Predicate<Record> filter = a -> a.getDim(key) == DUMMY_FILTER_DIM;
            for (Long v : value) {
                final long l = (long) v;
                filter = filter.or(a -> a.getDim(key) == l);
            }
            filters.add(filter);
        }
        
        Predicate<Record> andFilter = a -> a.getId() != DUMMY_FILTER_DIM;
        for (Predicate<Record> filter : filters) {
            andFilter = andFilter.and(filter);
        }
        
        return factTable.getRecords().parallelStream().filter(andFilter);
    }

    public BigDecimal sum(String indName) {
        
        return sum(indName, null);
    }
    
    public BigDecimal sum(String indName, Map<String, List<Long>> filterDims) {
        
        long enterTime = System.currentTimeMillis();
        
        Stream<Record> stream = filter(indName, filterDims);
        LOGGER.debug("Prepare predicate using {}ms.", System.currentTimeMillis() - enterTime);
        
        BigDecimal sum = stream.map(
            new Function<Record, BigDecimal>() {
                @Override
                public BigDecimal apply(Record t) {
                    return t.getInd(indName);
                }
            }).reduce(new BigDecimal(0), (x, y) -> x.add(y))
                .setScale(IND_SCALE, BigDecimal.ROUND_HALF_UP);
        LOGGER.info("Sum {} filter {} result {} using {} ms.", indName, filterDims, sum, System.currentTimeMillis() - enterTime);
        
        return sum;
    }
    
    public Map<Long, BigDecimal> group(String indName, String groupByDimName, Map<String, List<Long>> filterDims) {
        
        long enterTime = System.currentTimeMillis();
        Stream<Record> stream = filter(indName, filterDims);
        
        Map<Long, BigDecimal> group = new HashMap<Long, BigDecimal>();
        stream.collect(Collectors.groupingBy(p->p.getDim(groupByDimName), Collectors.reducing(new BigDecimal(0), 
                new Function<Record, BigDecimal>() {
                    @Override
                    public BigDecimal apply(Record t) {
                        return t.getInd(indName);
                    }
                }, (x, y) -> x.add(y))))
            .forEach((k, v) -> group.put(k, v.setScale(IND_SCALE, BigDecimal.ROUND_HALF_UP)));
        LOGGER.info("Group by {} sum {} filter {} result {} using {} ms.", groupByDimName, indName, filterDims, group, System.currentTimeMillis() - enterTime);
        return group;
    }

    @Override
    public String toString() {
        return "MiniCube [factTable=" + factTable + "]";
    }
    
}
