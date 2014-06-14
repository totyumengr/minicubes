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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Fact-table object of <a href="http://en.wikipedia.org/wiki/Star_schema">Star Schema</a>.
 * @author mengran
 *
 */
public class FactTable {
    
    private Meta meta;
    private Map<Long, Record> records;
    
    private static class Meta {
        
        private String name;
        private List<String> dimNames = new ArrayList<String>();
        private Map<String, Integer> indNames = new HashMap<String, Integer>();

        @Override
        public String toString() {
            return "Meta [name=" + name + ", dimNames=" + dimNames + ", indNames=" + indNames + "]";
        }
        
    }
    
    public class Record {
        
        private Long id;
        private List<Long> dimOfFact = new ArrayList<Long>();
        private List<BigDecimal> indOfFact = new ArrayList<BigDecimal>();
        
        private Record(Long id) {
            super();
            this.id = id;
        }

        public BigDecimal getInd(String indName) {
            
            int index = FactTable.this.getIndIndex(indName);
            return indOfFact.get(index);
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
        Map<Long, Record> records = new HashMap<Long, FactTable.Record>();
        
        this.meta = meta;
        this.records = records;
    }
    
    public static class FactTableBuilder {
        private static final ThreadLocal<FactTable> IN_BUILDING = new ThreadLocal<FactTable>();
        
        public FactTableBuilder build(String name) {
            
            if (IN_BUILDING.get() != null) {
                throw new IllegalStateException("Previous building " + IN_BUILDING.get() + " is doing call #done to finish it.");
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
            
            current.meta.dimNames.addAll(dimColumnNames);
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
        
        public FactTableBuilder addDimDatas(Long primaryKey, List<Long> dimDatas) {
            
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
        
        public FactTableBuilder addIndDatas(Long primaryKey, List<BigDecimal> indDatas) {
            
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
            
            IN_BUILDING.set(null);
            return current;
        }
    }
    
    public Collection<Record> getRecords() {
        return records.values();
    }

    /**
     * Indicate index by search {{@link #meta}.
     * @param indName Indicate names 
     * @return indicate values in fact-table
     * @throws IllegalArgumentException if indicate names is empty or invalid.
     */
    public int getIndIndex(String indName) throws IllegalArgumentException {
        
        if (indName == null || "".equals(indName) || !meta.indNames.containsKey(indName)) {
            throw new IllegalArgumentException();
        }
        
        int index = meta.indNames.get(indName);
        return index;
    }

    @Override
    public String toString() {
        return "FactTable [meta=" + meta + ", records=" + records + "]";
    }
    
}
