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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

import md.math.DoubleDouble;

import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.Ordered;
import org.springframework.util.Assert;

/**
 * Fact table object of <a href="http://en.wikipedia.org/wiki/Star_schema">Star Schema</a>. It hold detail data and 
 * need use huge memories of course.
 * 
 * @author mengran
 *
 */
public class FactTable {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(FactTable.class);
    
    Meta meta;
    /**
     * For speeding {@link FactTableBuilder}, clear after {@link FactTableBuilder#done()}.
     */
    private Map<Integer, Record> records;
    
    /**
     * Bitmap index for speed up aggregated calculation. Key is columnNames + ":" + dimValue
     */
    private Map<String, RoaringBitmap> bitmapIndex = new HashMap<String, RoaringBitmap>();
    
    /**
     * Protect fact-table merge action.
     */
    private ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    
    static class Meta {
        
        String name;
        private LinkedHashMap<String, Integer> indColumnNames = new LinkedHashMap<String, Integer>();
        private LinkedHashMap<String, Integer> dimColumnNames = new LinkedHashMap<String, Integer>();

        @Override
        public String toString() {
            return "Meta [name=" + name + ", indicator columnNames=" + indColumnNames
                    + ", dimension columnNames=" + dimColumnNames;
        }
    }
    
    /**
     * Holding detail data, streaming calculation target object.
     * @author mengran
     *
     */
    public class Record {
        
        private int id;     // Equal to PK. Can hold 2^31 records.
        /**
         * Use DoubleDouble for better performance. See http://tsusiatsoftware.net/dd/main.html
         */
        private DoubleDouble[] indOfFact = null;
        
        private int[] dimOfFact = null;
        
        private Record(Integer id) {
            super();
            this.id = id;
        }
        
        public int getId() {
            return id;
        }

        public DoubleDouble getInd(String indName) {
            
            int index = FactTable.this.getIndIndex(indName);
            return indOfFact[index];
        }
        
        public int getDim(String dimName) {
            
            final int index = FactTable.this.getDimIndex(dimName);
            return dimOfFact[index];
        }

        @Override
        public String toString() {
            return "Record [id=" + id + "]";
        }
        
    }
    
    private FactTable(String name) {
        // Internal
        Meta meta = new Meta();
        meta.name = name;
        Assert.hasText(name, "Fact-table name can not empty.");
        
        this.meta = meta;
        this.records = new HashMap<Integer, FactTable.Record>(0);;
    }
    
    /**
     * Issue-8 implementation.
     * @author mengran
     *
     */
    public static interface FactTableBuilderUserDefineDimProvider extends Ordered {
        
        /**
         * 
         * @return column and expr configuration for user define dimensions. MUST NOT NULL.
         */
        LinkedHashMap<String, String> getUserDefineDimConfig();
    }
    
    /**
     * Builder pattern class for {@link FactTable}, chain model begin with {@link #build(String, int)} 
     * and end with {{@link #done()}.
     * 
     * @author mengran
     *
     */
    public static class FactTableBuilder {
        private static final ThreadLocal<FactTable> IN_BUILDING = new ThreadLocal<FactTable>();
        
        private static List<FactTableBuilderUserDefineDimProvider> providers = new ArrayList<FactTableBuilderUserDefineDimProvider>();
        private static ScriptEngine scriptEngine;
        
        static {
            ServiceLoader<FactTableBuilderUserDefineDimProvider> serviceLoader = ServiceLoader.load(FactTableBuilderUserDefineDimProvider.class);
            for (Iterator<FactTableBuilderUserDefineDimProvider> it = serviceLoader.iterator();it.hasNext();) {
                providers.add(it.next());
            }
            Collections.sort(providers, new Comparator<FactTableBuilderUserDefineDimProvider>() {
                
                @Override
                public int compare(FactTableBuilderUserDefineDimProvider o1,
                        FactTableBuilderUserDefineDimProvider o2) {
                    return o1.getOrder() - o2.getOrder();
                }
            });
            LOGGER.info("Retrieve user define dimension providers {}", providers);
            
            scriptEngine = new ScriptEngineManager().getEngineByName("nashorn");
            
            for (FactTableBuilderUserDefineDimProvider p : providers) {
                for(Entry<String, String> e : p.getUserDefineDimConfig().entrySet()) {
                    try {
                        scriptEngine.eval(e.getValue());
                        LOGGER.info("regist user-define column {} expr {}", e.getKey(), e.getValue());
                    } catch (Exception e1) {
                        LOGGER.error("Error occurred when try to process user-define column {} expr {}", e.getKey(), e.getValue());
                        throw new RuntimeException(e1);
                    }
                }
            }
        }
        
        /**
         * Constructor
         */
        public FactTableBuilder() {
            super();
            // FIXME: Need specify strict build method call-flow
        }

        public FactTableBuilder build(String name) {
            
            if (IN_BUILDING.get() != null) {
                throw new IllegalStateException("Previous building " + IN_BUILDING.get()
                    + " is doing call #done to finish it.");
            }
            // FIXME: Check name?
            
            IN_BUILDING.set(new FactTable(name));
            return this;
        }
        
        public FactTableBuilder addDimColumns(List<String> dimColumnNames) {
            
            FactTable current = IN_BUILDING.get();
            if (current == null) {
                throw new IllegalStateException("Current building is not started, call #build first.");
            }
            
            for (int i = 0; i < dimColumnNames.size(); i++) {
                if (current.meta.dimColumnNames.keySet().contains(dimColumnNames.get(i))) {
                    throw new IllegalStateException("Dimension " + dimColumnNames.get(i) + " has exists.");
                }
                current.meta.dimColumnNames.put(dimColumnNames.get(i), current.meta.dimColumnNames.size());
            }
            
            // Add user-define dimension process
            for (FactTableBuilderUserDefineDimProvider p : providers) {
                for (String key : p.getUserDefineDimConfig().keySet()) {
                    current.meta.dimColumnNames.put(key, current.meta.dimColumnNames.size());
                }
            }
            LOGGER.info("Complete filling user-define dimension and now dimension columns is {}", current.meta.dimColumnNames);
            
            return this;
        }
        
        public FactTableBuilder addIndColumns(List<String> indColumnNames) {
            
            FactTable current = IN_BUILDING.get();
            if (current == null) {
                throw new IllegalStateException("Current building is not started, call #build first.");
            }
            
            for (int i = 0; i < indColumnNames.size(); i++) {
                if (current.meta.indColumnNames.keySet().contains(indColumnNames.get(i))) {
                    throw new IllegalStateException("Indication " + indColumnNames.get(i) + " has exists.");
                }
                current.meta.indColumnNames.put(indColumnNames.get(i), current.meta.indColumnNames.size());
            }
            return this;
        }
        
        public FactTableBuilder addDimDatas(Integer primaryKey, List<Integer> dimDatas) {
            
            FactTable current = IN_BUILDING.get();
            if (current == null) {
                throw new IllegalStateException("Current building is not started, call #build first.");
            }
            Assert.isTrue(current.meta.dimColumnNames.size() > 0, "Fact-table must have a dimension column at least.");
            
            Record record = current.records.get(primaryKey);
            if (record == null) {
                record = current.new Record(primaryKey);
                current.records.put(primaryKey, record);
            }
            
            // Fill dimension data
            fillDimDatas(current, record, dimDatas);
            
            return this;
        }
        
        private void fillDimDatas(FactTable current, Record record, List<Integer> dimDatas) {
            // Build bitmap index
            int baseIndex = record.dimOfFact == null ? 0 : record.dimOfFact.length - dimDatas.size();
            for (int i = 0; i < dimDatas.size(); i++) {
                Integer dimValue = dimDatas.get(i);
                int fi = baseIndex + i;
                // Fill dimension value
                if (record.dimOfFact == null) {
                    record.dimOfFact = new int[current.meta.dimColumnNames.size()];
                }
//                if (record.dimOfFact.length < fi) {
//                    // Expand dimension array
//                    int[] array = new int[record.dimOfFact.length + dimDatas.size()];
//                    System.arraycopy(record.dimOfFact, 0, array, 0, record.dimOfFact.length);
//                    record.dimOfFact = array;
//                }
                record.dimOfFact[fi] = dimValue;
                
                // Index dimension value
                String column = current.meta.dimColumnNames.entrySet().stream().filter(v -> v.getValue() == fi).findFirst().get().getKey();
                String bitMapkey = column + ":" + dimValue;
                RoaringBitmap bitmap = current.bitmapIndex.get(bitMapkey);
                if (bitmap == null) {
                    bitmap = new RoaringBitmap();
                    current.bitmapIndex.put(bitMapkey, bitmap);
                }
                bitmap.add(record.getId());
            }
        }
        
        public FactTableBuilder addIndDatas(Integer primaryKey, List<DoubleDouble> indDatas) {
            
            FactTable current = IN_BUILDING.get();
            if (current == null) {
                throw new IllegalStateException("Current building is not started, call #build first.");
            }
            Record record = current.records.get(primaryKey);
            if (record == null) {
                record = current.new Record(primaryKey);
                current.records.put(primaryKey, record);
            }
            record.indOfFact = indDatas.toArray(new DoubleDouble[0]);
            if (record.indOfFact.length != current.meta.indColumnNames.size()) {
                throw new IllegalStateException("Current version only support one-time indicator data filling.");
            }
            
            // Add user-define dimension process
            int i = -1;
            for (String indColumn : current.meta.indColumnNames.keySet()) {
                i++;
                scriptEngine.put(indColumn, indDatas.get(i).doubleValue());
            }
            
            List<Integer> userDefineDimensions = new ArrayList<Integer>();
            for (FactTableBuilderUserDefineDimProvider p : providers) {
                for(Entry<String, String> e : p.getUserDefineDimConfig().entrySet()) {
                    try {
                        Invocable inv = (Invocable) scriptEngine;
                        Object o = inv.invokeFunction(e.getKey(), new Object[0]);
                        userDefineDimensions.add(Integer.valueOf(o.toString()));
                        LOGGER.debug("process user-define column {} expr {} value {}", e.getKey(), e.getValue(), o);
                    } catch (Exception e1) {
                        LOGGER.error("Error occurred when try to process user-define column {} expr {}", e.getKey(), e.getValue());
                        throw new RuntimeException(e1);
                    }
                }
            }
            fillDimDatas(current, record, userDefineDimensions);
            
            return this;
        }
        
        public FactTable done() {
            
            FactTable current = IN_BUILDING.get();
            if (current == null) {
                throw new IllegalStateException("Current building is not started, call #build first.");
            }
            IN_BUILDING.set(null);
            
            Set<String> allNames = new HashSet<String>();
            allNames.addAll(current.meta.dimColumnNames.keySet());
            allNames.addAll(current.meta.indColumnNames.keySet());
            Assert.isTrue(allNames.size() == current.meta.dimColumnNames.size() + current.meta.indColumnNames.size(), 
                    "Contains same name between dimentions and indicators.");
            
            int usedKb = 0;
            int usedBytes = 0;
            for (Entry<String, RoaringBitmap> e : current.bitmapIndex.entrySet()) {
                e.getValue().trim();
                if (usedBytes > (1024 * 1024 * 1024)) {
                    usedKb = usedKb + (usedBytes / 1024);
                    usedBytes = 0;
                }
                usedBytes = usedBytes + e.getValue().getSizeInBytes();
                LOGGER.debug("Index for {} of {} records", e.getKey(), e.getValue().getCardinality());
            }
            usedKb = usedKb + (usedBytes / 1024);
            LOGGER.info("Build completed: name {} with {} dimension columns, {} measure columns and {} records, {} indexes used {} kb.", 
                    current.meta.name, current.meta.dimColumnNames.size(), current.meta.indColumnNames.size(), 
                    current.records.size(), current.bitmapIndex.size(), usedKb);
            
            return current;
        }
    }
    
    /**
     * @return records of key "records" and indexes of key "bitmapIndex".
     */
    Map<String, Object> getData() {
        try {
            readWriteLock.readLock().lock();
            Map<String, Object> data = new HashMap<String, Object>(2);
            data.put("records", records);
            data.put("bitmapIndex", bitmapIndex);
            return data;    
        } finally {
            readWriteLock.readLock().unlock();
        }
    }
    
    /**
     * @param merge fact-table will be merge into.
     * @throws IllegalArgumentException when parameter is null
     * @since 0.2
     */
    void merge(FactTable merge) {
        
        if (merge == null) {
            throw new IllegalArgumentException();
        }
        LOGGER.info("Try to merge {} into {}.", merge, this);
        try {
            readWriteLock.writeLock().lock();
            // Start merge
            for (Entry<Integer, Record> entry : merge.records.entrySet()) {
                this.records.put(entry.getKey(), entry.getValue());
            }
            for (Entry<String, RoaringBitmap> entry : merge.bitmapIndex.entrySet()) {
                this.bitmapIndex.merge(entry.getKey(), entry.getValue(), 
                        new BiFunction<RoaringBitmap, RoaringBitmap, RoaringBitmap>() {

                    @Override
                    public RoaringBitmap apply(RoaringBitmap t, RoaringBitmap u) {
                        return RoaringBitmap.or(t, u);
                    }
                });
            }
        } finally {
            readWriteLock.writeLock().unlock();
        }
        
        LOGGER.info("Merge {} successfully into {}.", merge, this);
        return;
    }

    /**
     * Indicate index by search {{@link #meta}, high performance is very important.
     * @param indName Indicate names 
     * @return indicate index in fact-table
     * @throws IllegalArgumentException if indicate names is empty or invalid.
     */
    public int getIndIndex(String indName) throws IllegalArgumentException {
        
        int index = -1;
        if (indName == null || "".equals(indName) || (index = meta.indColumnNames.get(indName)) < 0) {
            throw new IllegalArgumentException();
        }
        
        return index;
    }
    
    /**
     * Dimension index by search {{@link #meta}, high performance is very important.
     * @param dimName Dimension names 
     * @return dimension index in fact-table
     * @throws IllegalArgumentException if indicate names is empty or invalid.
     */
    public int getDimIndex(String dimName) throws IllegalArgumentException {
        
        int index = -1;
        if (dimName == null || "".equals(dimName) || (index = meta.dimColumnNames.get(dimName)) < 0) {
            throw new IllegalArgumentException();
        }
        return index;
    }
    
    @Override
    public String toString() {
        return "FactTable [meta=" + meta + ", records=" + records.size() + "]";
    }
    
}
