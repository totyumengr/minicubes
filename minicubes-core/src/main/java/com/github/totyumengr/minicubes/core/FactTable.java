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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;

import md.math.DoubleDouble;

import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    
    private Meta meta;
    /**
     * For speeding {@link FactTableBuilder}, clear after {@link FactTableBuilder#done()}.
     */
    private Map<Integer, Record> records;
    
    /**
     * Bitmap index for speed up aggregated calculation
     */
    Map<String, RoaringBitmap> bitmapIndex = new HashMap<String, RoaringBitmap>();
    
    private static class Meta {
        
        private String name;
        private List<String> columnNames = new ArrayList<>();
        private Map<String, Integer> columnNameIndexMap = new HashMap<String, Integer>();
        private int indStartIndex = -1;
        
        @Override
        public String toString() {
            return "Meta [name=" + name + ", columnNames=" + columnNames
                    + ", indStartIndex=" + indStartIndex + "]";
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
            return indOfFact[index - meta.indStartIndex];
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
    
    private FactTable(String name, int indStartIndex) {
        // Internal
        Meta meta = new Meta();
        meta.name = name;
        meta.indStartIndex = indStartIndex;
        Assert.hasText(name, "Fact-table name can not empty.");
        
        this.meta = meta;
        this.records = new HashMap<Integer, FactTable.Record>(0);;
    }
    
    /**
     * Builder pattern class for {@link FactTable}, chain model begin with {{@link #build(String)} 
     * and end with {{@link #done()}.
     * 
     * @author mengran
     *
     */
    public static class FactTableBuilder {
        private static final ThreadLocal<FactTable> IN_BUILDING = new ThreadLocal<FactTable>();
        
        public FactTableBuilder build(String name, int indStartIndex) {
            
            if (IN_BUILDING.get() != null) {
                throw new IllegalStateException("Previous building " + IN_BUILDING.get()
                    + " is doing call #done to finish it.");
            }
            // FIXME: Check name?
            
            IN_BUILDING.set(new FactTable(name, indStartIndex));
            return this;
        }
        
        public FactTableBuilder addDimColumns(List<String> dimColumnNames) {
            
            FactTable current = IN_BUILDING.get();
            if (current == null) {
                throw new IllegalStateException("Current building is not started, call #build first.");
            }
            
            if (current.meta.columnNames.size() > current.meta.indStartIndex && current.meta.indStartIndex > 0) {
                throw new IllegalStateException("Have filled " + current.meta.indStartIndex + 
                        " dimensions yet.");
            }
            
            for (int i = 0; i < dimColumnNames.size(); i++) {
                if (current.meta.columnNames.contains(dimColumnNames.get(i))) {
                    throw new IllegalStateException("Dimension " + dimColumnNames.get(i) + " has exists.");
                }
                current.meta.columnNames.add(dimColumnNames.get(i));
            }
            
            // Because Integer.MAX_VALUE is 2147483647
            if (current.meta.columnNames.size() > 21) {
                throw new IllegalStateException("Current version only support 20 dimensions max.");
            }
            
            return this;
        }
        
        public FactTableBuilder addIndColumns(List<String> indColumnNames) {
            
            FactTable current = IN_BUILDING.get();
            if (current == null) {
                throw new IllegalStateException("Current building is not started, call #build first.");
            }
            
            if (current.meta.indStartIndex < 0) {
                // Set flag for specify indication 
                current.meta.indStartIndex = current.meta.columnNames.size();
            }
            
            if (current.meta.columnNames.size() < current.meta.indStartIndex && current.meta.indStartIndex > 0) {
                throw new IllegalStateException("Please fill " + current.meta.indStartIndex + 
                        " dimensions first, current dimension size is " + current.meta.columnNames.size());
            }
            
            for (int i = 0; i < indColumnNames.size(); i++) {
                if (current.meta.columnNames.contains(indColumnNames.get(i))) {
                    throw new IllegalStateException("Indication " + indColumnNames.get(i) + " has exists.");
                }
                current.meta.columnNames.add(indColumnNames.get(i));
            }
            return this;
        }
        
        public FactTableBuilder addDimDatas(Integer primaryKey, List<Integer> dimDatas) {
            
            FactTable current = IN_BUILDING.get();
            if (current == null) {
                throw new IllegalStateException("Current building is not started, call #build first.");
            }
            if (dimDatas.size() < current.meta.indStartIndex) {
                throw new IllegalStateException("Current version only support one-time dimension data setting.");
            }
            Assert.isTrue(current.meta.indStartIndex > 0, "Fact-table must have a dimension column at least.");
            
            Record record = current.records.get(primaryKey);
            if (record == null) {
                record = current.new Record(primaryKey);
                current.records.put(primaryKey, record);
            }
            
            // Build bitmap index
            for (int i = 0; i < dimDatas.size(); i++) {
                Integer dimValue = dimDatas.get(i);
                String bitMapkey = current.meta.columnNames.get(i) + ":" + dimValue;
                RoaringBitmap bitmap = current.bitmapIndex.get(bitMapkey);
                if (bitmap == null) {
                    bitmap = new RoaringBitmap();
                    current.bitmapIndex.put(bitMapkey, bitmap);
                }
                bitmap.add(record.getId());
                
                // Index dimension value
                if (record.dimOfFact == null) {
                    record.dimOfFact = new int[current.meta.indStartIndex];
                }
                record.dimOfFact[i] = dimValue;
            }
            
            return this;
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
            
            return this;
        }
        
        public FactTable done() {
            
            FactTable current = IN_BUILDING.get();
            if (current == null) {
                throw new IllegalStateException("Current building is not started, call #build first.");
            }

            IN_BUILDING.set(null);
//            current.recordList = Collections.unmodifiableCollection(current.records.values());
//            current.records = new HashMap<Integer, Record>(0);
            
            // FIXME: Check valid or not
            current.meta.columnNames.stream().forEach(new Consumer<String>() {
                @Override
                public void accept(String t) {
                    Integer index = current.meta.columnNameIndexMap.put(t, current.meta.columnNameIndexMap.size());
                    Assert.isTrue(index == null, "Found the same column name " + t);
                }
            });
            
            for (Entry<String, RoaringBitmap> e : current.bitmapIndex.entrySet()) {
                LOGGER.debug("Index for {} of {} records", e.getKey(), e.getValue().getCardinality());
            }
            LOGGER.info("Build completed: name {} with {} dimension columns, {} measure columns and {} records, {} indexes.", 
                    current.meta.name, current.meta.indStartIndex, current.meta.columnNames.size() - current.meta.indStartIndex, 
                    current.records.size(), current.bitmapIndex.size());
            
            return current;
        }
    }
    
    /**
     * @return Unmodifiable records.
     * @see Collections#unmodifiableCollection(Collection)
     */
    Map<Integer, Record> getRecords() {
        return records;
    }

    /**
     * Indicate index by search {{@link #meta}, high performance is very important.
     * @param indName Indicate names 
     * @return indicate index in fact-table
     * @throws IllegalArgumentException if indicate names is empty or invalid.
     */
    public int getIndIndex(String indName) throws IllegalArgumentException {
        
        int index = -1;
        if (indName == null || "".equals(indName) || (index = meta.columnNameIndexMap.get(indName)) < 0) {
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
        if (dimName == null || "".equals(dimName) || (index = meta.columnNameIndexMap.get(dimName)) < 0) {
            throw new IllegalArgumentException();
        }
        return index;
    }
    
    @Override
    public String toString() {
        return "FactTable [meta=" + meta + ", records=" + records + "]";
    }
    
}
