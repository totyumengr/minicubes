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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Fact table object of <a href="http://en.wikipedia.org/wiki/Star_schema">Star Schema</a>. It hold detail data and 
 * need use huge memories of course.
 * 
 * @author mengran
 *
 */
public class FactTable {
    
    private Meta meta;
    /**
     * For speeding {@link FactTableBuilder}, clear after {@link FactTableBuilder#done()}.
     */
    private Map<Integer, Record> records;
    private Collection<Record> recordList;
    
    private static class Meta {
        
        private String name;
        private Map<String, Integer> dimNames = new HashMap<String, Integer>();
        private Map<String, Integer> indNames = new HashMap<String, Integer>();

        @Override
        public String toString() {
            return "Meta [name=" + name + ", dimNames=" + dimNames + ", indNames=" + indNames + "]";
        }
        
    }
    
    /**
     * Holding detail data, streaming calculation target object.
     * @author mengran
     *
     */
    public class Record {
        
        private Integer id;     // Equal to PK. Can hold 2^31 records.
        /**
         * FIXME: Use integer for less memory usage? 2^31-1 maybe not enough.
         */
        private List<Long> dimOfFact = new ArrayList<Long>();
        /**
         * FIXME: Use double for better performance? Precision is problem.
         */
        private List<BigDecimal> indOfFact = new ArrayList<BigDecimal>();
        
        private Record(Integer id) {
            super();
            this.id = id;
        }
        
        public Integer getId() {
            
            return id;
        }

        public BigDecimal getInd(String indName) {
            
            int index = FactTable.this.getIndIndex(indName);
            return indOfFact.get(index);
        }
        
        public Long getDim(String dimName) {
            
            int index = FactTable.this.getDimIndex(dimName);
            long dim = dimOfFact.get(index);
            return dim;
        }

        @Override
        public String toString() {
            return "Record [id=" + id + ", dimOfFact=" + dimOfFact + ", indOfFact=" + indOfFact
                + "]";
        }
        
    }
    
    private FactTable(String name) {
        // Internal
        Meta meta = new Meta();
        meta.name = name;
        
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
            // FIXME: Check exists?
            
            int index = current.meta.dimNames.size();
            for (int i = 0; i < dimColumnNames.size(); i++) {
                current.meta.dimNames.put(dimColumnNames.get(i), index + i);
            }
            return this;
        }
        
        public FactTableBuilder addIndColumns(List<String> indColumnNames) {
            
            FactTable current = IN_BUILDING.get();
            if (current == null) {
                throw new IllegalStateException("Current building is not started, call #build first.");
            }
            // FIXME: Check exists?
            
            int index = current.meta.indNames.size();
            for (int i = 0; i < indColumnNames.size(); i++) {
                current.meta.indNames.put(indColumnNames.get(i), index + i);
            }
            return this;
        }
        
        public FactTableBuilder addDimDatas(Integer primaryKey, List<Long> dimDatas) {
            
            FactTable current = IN_BUILDING.get();
            if (current == null) {
                throw new IllegalStateException("Current building is not started, call #build first.");
            }
            Record record = current.records.get(primaryKey);
            if (record == null) {
                record = current.new Record(primaryKey);
                current.records.put(primaryKey, record);
            }
            record.dimOfFact.addAll(dimDatas);
            
            return this;
        }
        
        public FactTableBuilder addIndDatas(Integer primaryKey, List<BigDecimal> indDatas) {
            
            FactTable current = IN_BUILDING.get();
            if (current == null) {
                throw new IllegalStateException("Current building is not started, call #build first.");
            }
            Record record = current.records.get(primaryKey);
            if (record == null) {
                record = current.new Record(primaryKey);
                current.records.put(primaryKey, record);
            }
            record.indOfFact.addAll(indDatas);
            
            return this;
        }
        
        public FactTable done() {
            
            FactTable current = IN_BUILDING.get();
            if (current == null) {
                throw new IllegalStateException("Current building is not started, call #build first.");
            }
            
            // FIXME: Check valid or not
            
            IN_BUILDING.set(null);
            current.recordList = Collections.unmodifiableCollection(current.records.values());
            current.records = new HashMap<Integer, Record>(0);
            
            return current;
        }
    }
    
    /**
     * @return Unmodifiable records.
     * @see Collections#unmodifiableCollection(Collection)
     */
    public Collection<Record> getRecords() {
        return recordList;
    }

    /**
     * Indicate index by search {{@link #meta}, high performance is very important.
     * @param indName Indicate names 
     * @return indicate index in fact-table
     * @throws IllegalArgumentException if indicate names is empty or invalid.
     */
    public int getIndIndex(String indName) throws IllegalArgumentException {
        
        int index = -1;
        if (indName == null || "".equals(indName) || (index = meta.indNames.get(indName)) < 0) {
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
        if (dimName == null || "".equals(dimName) || (index = meta.dimNames.get(dimName)) < 0) {
            throw new IllegalArgumentException();
        }
        return index;
    }

    @Override
    public String toString() {
        return "FactTable [meta=" + meta + ", records=" + recordList + "]";
    }
    
}
