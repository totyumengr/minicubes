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
import java.util.function.Function;

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
    
    private FactTable factTable;

    // FIXME: Add dimension table
    public MiniCube(FactTable factTable) {
        super();
        this.factTable = factTable;
    }

    public BigDecimal sum(String indName) {
        
        long enterTime = System.currentTimeMillis();
        BigDecimal sum = factTable.getRecords().parallelStream().map(
            new Function<Record, BigDecimal>() {

                @Override
                public BigDecimal apply(Record t) {
                    
                    return t.getInd(indName);
                }
            
            }).reduce(new BigDecimal(0), (x, y) -> x.add(y))
                .setScale(IND_SCALE, BigDecimal.ROUND_HALF_UP);
        
        LOGGER.info("{} sum {} using {} ms.", indName, sum, System.currentTimeMillis() - enterTime);
        return sum;
    }

    @Override
    public String toString() {
        return "MiniCube [factTable=" + factTable + "]";
    }
    
}
