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
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import md.math.DoubleDouble;

import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.totyumengr.minicubes.core.FactTable.Record;

/**
 * In-memory cube base on java8 stream feature and use memory calculation for best performance(millisecond level).
 * Distributed architect is important, {@link MiniCube} design as unit participant, the one logic level cube will 
 * be API for users.
 * 
 * <p>{@link MiniCube} design for easily fast transfer between cluster nodes to support fail-safe feature.
 * 
 * <p>Add bitmap index for speed up aggregated calculation, use <a href="https://github.com/lemire/RoaringBitmap">RoaringBitmap</a>.
 * 
 * @author mengran
 * 
 * @see #DUMMY_FILTER_DIM
 *
 */
public class MiniCube implements Aggregations {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(MiniCube.class);
    
    /**
     * Take care this value, make sure do not has this values in data-set.
     */
    public static final int DUMMY_FILTER_DIM = -999999999;
    
    FactTable factTable;

    // FIXME: Add dimension table
    public MiniCube(FactTable factTable) {
        super();
        this.factTable = factTable;
    }
    
    // ---------------------------- Aggregation API ----------------------------
    
    private Stream<Entry<Integer, Record>> filter(Map<String, List<Integer>> filterDims) {
        
        if (filterDims == null) {
            filterDims = new HashMap<String, List<Integer>>(0);
        }
        
        List<Predicate<Entry<Integer, Record>>> filters = new ArrayList<Predicate<Entry<Integer, Record>>>(filterDims.size());
        
        RoaringBitmap ands = null;
        for (Entry<String, List<Integer>> entry : filterDims.entrySet()) {
            RoaringBitmap ors = new RoaringBitmap();
            for (Integer v : entry.getValue()) {
                RoaringBitmap o = factTable.bitmapIndex.get(entry.getKey() + ":" + v);
                if (o != null) {
                    ors.or(o);
                } else {
                    LOGGER.debug("Can not find bitmap index for " + entry.getKey() + ":" + v);
                }
            }
            if (ands == null) {
                ands = ors;
            } else {
                ands.and(ors);
            }
        }
        if (ands != null) {
            // FIXME: Always false. 
            if (ands.getCardinality() > Integer.MAX_VALUE) {
                throw new UnsupportedOperationException();
//                for (Entry<String, List<Integer>> entry : filterDims.entrySet()) {
//                    String key = entry.getKey();
//                    List<Integer> value = entry.getValue();
//                    Predicate<Record> filter = a -> a.getDim(key) == DUMMY_FILTER_DIM;
//                    for (Integer v : value) {
//                        filter = filter.or(a -> a.getDim(key) == v);
//                    }
//                    filters.add(filter);
//                 }
            } else {
                final RoaringBitmap m = ands;
                Stream<Entry<Integer, Record>> stream = factTable.getRecords().entrySet().parallelStream().filter(
                        new Predicate<Entry<Integer, Record>>() {

                            @Override
                            public boolean test(Entry<Integer, Record> t) {
                                return m.contains(t.getKey());
                            }
                        });
                LOGGER.info("Filter record IDs count {}", ands.getCardinality());
                return stream;
            }
        }
        
        Predicate<Entry<Integer, Record>> andFilter = i -> i.getKey() != DUMMY_FILTER_DIM;
        for (Predicate<Entry<Integer, Record>> filter : filters) {
            andFilter = andFilter.and(filter);
        }
        
        return factTable.getRecords().entrySet().parallelStream().filter(andFilter);
    }

    /**
     * Sum calculation of given indicate with filter. It equal to "SELECT SUM({indName}) FROM {fact table of cube}".
     * @param indName indicate name for sum
     * @return result that formated using {@value #IND_SCALE}
     */
    @Override
    public BigDecimal sum(String indName) {
        
        // Delegate to overload method
        return sum(indName, null);
    }
    
    /**
     * Sum calculation of given indicate with filter. It equal to "SELECT SUM({indName}) FROM {fact table of cube} WHERE 
     * {dimension1 IN (a, b, c)} AND {dimension2 IN (d, e, f)}".
     * @param indName indicate name for sum
     * @param filterDims filter dimensions
     * @return result that formated using {@value #IND_SCALE}
     */
    @Override
    public BigDecimal sum(String indName, Map<String, List<Integer>> filterDims) {
        
        long enterTime = System.currentTimeMillis();
        
        Stream<Entry<Integer, Record>> stream = filter(filterDims);
        LOGGER.debug("Prepare predicate using {} ms.", System.currentTimeMillis() - enterTime);
        
        DoubleDouble sum = stream.map(
            new Function<Entry<Integer, Record>, DoubleDouble>() {
                @Override
                public DoubleDouble apply(Entry<Integer, Record> t) {
                    return t.getValue().getInd(indName);
                }
            }).reduce(new DoubleDouble(), (x, y) -> x.add(y));
        
        enterTime = System.currentTimeMillis() - enterTime;
        LOGGER.info("Sum {} filter {} result {} using {} ms.", indName, filterDims, sum, enterTime);
        
        return new BigDecimal(sum.toSciNotation()).setScale(IND_SCALE, BigDecimal.ROUND_HALF_UP);
    }
    
    @Override
    public Map<Integer, BigDecimal> sum(String indName, String groupByDimName, Map<String, List<Integer>> filterDims) {
        
        long enterTime = System.currentTimeMillis();
        Stream<Entry<Integer, Record>> stream = filter(filterDims);
        
        Map<Integer, BigDecimal> group = new HashMap<Integer, BigDecimal>();
        stream.collect(Collectors.groupingBy(p->p.getValue().getDim(groupByDimName), Collectors.reducing(new DoubleDouble(), 
                new Function<Entry<Integer, Record>, DoubleDouble>() {
                    @Override
                    public DoubleDouble apply(Entry<Integer, Record> t) {
                        return t.getValue().getInd(indName);
                    }
                }, (x, y) -> x.add(y))))
            .forEach((k, v) -> group.put(k, new BigDecimal(v.toSciNotation()).setScale(IND_SCALE, BigDecimal.ROUND_HALF_UP)));
        
        enterTime = System.currentTimeMillis() - enterTime;
        LOGGER.debug("Group by {} sum {} filter {} result {} using {} ms.", groupByDimName, indName, filterDims, group, 
                enterTime);
        LOGGER.info("Group by {} sum {} filter {} result size {} using {} ms.", groupByDimName, indName, 
                filterDims, group.size(), enterTime);
        return group;
    }

    @Override
    public String toString() {
        return "MiniCube [factTable=" + factTable + "]";
    }
    
    private RoaringBitmap buildBitMap(Set<Integer> set) {
        
        RoaringBitmap result = new RoaringBitmap();
        set.stream().forEach(x -> result.add(x));
        return result;
    }

    @Override
    public Map<Integer, RoaringBitmap> distinct(String distinctName, boolean isDim,
            String groupByDimName, Map<String, List<Integer>> filterDims) {
        
        long enterTime = System.currentTimeMillis();
        Stream<Entry<Integer, Record>> stream = filter(filterDims);
        Map<Integer, RoaringBitmap> group = new HashMap<Integer, RoaringBitmap>();
        // FIXME: indicator's distinct???
        stream.collect(Collectors.groupingBy(p->p.getValue().getDim(groupByDimName), 
                Collectors.mapping(isDim ? p->p.getValue().getDim(distinctName) 
                        : p->p.getValue().getInd(distinctName).intValue(), Collectors.toSet())))
              .forEach((k, v) -> group.put(k, buildBitMap(v)));
        enterTime = System.currentTimeMillis() - enterTime;
        LOGGER.debug("Group by {} distinct {} filter {} result {} using {} ms.", groupByDimName, distinctName, 
                filterDims, group, enterTime);
        LOGGER.info("Group by {} distinct {} filter {} result size {} using {} ms.", groupByDimName, distinctName, 
                filterDims, group.size(), enterTime);
        return group;
    }

    @Override
    public Map<Integer, Integer> discnt(String distinctName, boolean isDim, String groupByDimName,
            Map<String, List<Integer>> filterDims) {
        
        // Do distinct
        Map<Integer, RoaringBitmap> distinct = distinct(distinctName, isDim, groupByDimName, filterDims);
        // Count it
        Map<Integer, Integer> result = distinct.entrySet().stream().collect(
                Collectors.toMap(e -> e.getKey(), e -> e.getValue().getCardinality()));
        
        LOGGER.debug("Distinct {} with filter {} results is {}", distinct, filterDims, result);
        return result;
    }
    
}
