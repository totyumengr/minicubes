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

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

import com.github.totyumengr.minicubes.core.Aggregations;
import com.github.totyumengr.minicubes.core.MiniCube;

/**
 * It's design as a prophet in {@link MiniCube Cube World} that known all of things about {@link MiniCube MiniCubes}.
 * 
 * <p>One {@link MiniCube} live in one JVM and the converse is the same, simply say: one-2-one relationship.
 * Id of cube will be assigned when it came into {@link TimeSeriesMiniCubeManager}, the format is: {cube date}::{cube name}@{node name}.
 * <ul>
 *   <li>cube date: time dimension member name, for example 20140201, 201402, 2014Q1, 2014H1, 2014. <code>?</code> if not be assigned yet.
 *   <li>cube name: In general it's contains dimension name and indicator name. <code>cluster name </code> if not assign.
 *   <li>node name: JVM name, {@link com.hazelcast.core.Member Hazelcast member} in our default implementation
 * </ul>
 * <p>Unified dimension is important in analysis scene, so one cube cluster has <b>only one</b> dimension group.
 * 
 * <p>What means time-series cube? Shard by date time.
 * @author mengran
 *
 */
public interface TimeSeriesMiniCubeManager extends Aggregations {
    
    /**
     * @return All of (assigned) cube names in cluster.
     * {@link #reassignRole(String, String)}
     */
    Collection<String> allCubeIds();
    
    /**
     * @param cubeDate time-series field for locate cubes.
     * @return (assigned) cube IDs in cluster.
     * {@link #reassignRole(String, String)}
     */
    Collection<String> cubeIds(String cubeDate);
    
    /**
     * Re-assign a role to local cube and it will request data from data source:
     * report back to cluster when completed, and then accept request.
     * @param cubeId ID of cube
     * @param timeSeries role
     * @return new cube ID.
     */
    String reassignRole(String cubeId, String timeSeries);
    
    /**
     * @param task execute target
     * @param <T> result type
     * @param cubeIds execution on. Empty means <b>all members</b>
     * @param timeoutSeconds timeout seconds. &lt;=0 will be reuse {@link Integer#MAX_VALUE}
     * @return result list of every cube
     */
    <T> List<T> execute(Callable<T> task, Collection<String> cubeIds, int timeoutSeconds);
    
    /**
     * This is a stateful, wrapper method. Directly call {@link Aggregations}'s method means execute in <b>local</b> node.
     * So if you want to run in cluster, you <b>must</b> call this method first.
     * @param timeSeries set execution on
     * @return aggregation object
     */
    TimeSeriesMiniCubeManager aggs(String... timeSeries);
    
    /**
     * @param parallelModel specify Java8 Stream mode.
     * {@link Stream#isParallel()}
     */
    void setMode(boolean parallelModel);
    
}
