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
import java.util.List;
import java.util.Map;

/**
 * <p>FIXME: Need re-design aggregations API, make it fluent and rich for calculating.
 * 
 * <p>Define supported calculation operations.
 * @author mengran
 *
 */
public interface Aggregations {

    /**
     * Calculation scale
     */
    int IND_SCALE = 8;
    
    /**
     * Sum calculation of given indicate with filter. It equal to "SELECT SUM({indName}) FROM {fact table of cube}".
     * @param indName indicate name for sum
     * @return result of sum operation
     */
    BigDecimal sum(String indName);
    
    /**
     * Sum calculation of given indicate with filter. It equal to "SELECT SUM({indName}) FROM {fact table of cube} WHERE 
     * {dimension1 IN (a, b, c)} AND {dimension2 IN (d, e, f)}".
     * @param indName indicate name for sum
     * @param filterDims filter dimensions
     * @return result of sum operation
     */
    BigDecimal sum(String indName, Map<String, List<Integer>> filterDims);
    
    /**
     * Sum calculation of given indicate with filter and grouper. It equal to "SELECT SUM({indName}) FROM {fact table of cube} WHERE 
     * {dimension1 IN (a, b, c)} AND {dimension2 IN (d, e, f)} group by {dimension3}".
     * @param indName indicate name for sum
     * @param groupByDimName group by dimensions
     * @param filterDims filter dimensions
     * @return result of sum operation
     */
    Map<Integer, BigDecimal> sum(String indName, String groupByDimName, Map<String, List<Integer>> filterDims);
    
}
