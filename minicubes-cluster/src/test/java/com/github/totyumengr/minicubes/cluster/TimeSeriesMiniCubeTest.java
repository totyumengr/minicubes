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

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.mashape.unirest.http.Unirest;

/**
 * @author mengran
 *
 */
public class TimeSeriesMiniCubeTest {
    
    @BeforeClass
    public static void beforeClass() {
        Application.main(new String[0]);
    }
    
    @Test
    public void test() {
        
        System.out.println("Start test...");
        
        HazelcastInstance server = DiscardListener.applicationContext.getBean(HazelcastInstance.class);
        ILock lock = server.getLock("TimeSeriesMiniCubeTest");
        boolean runTest = false;
        lock.lock();
        try {
            Object executor = server.getMap("TimeSeriesMiniCubeTest_map").get("executor");
            if (executor == null) {
                server.getMap("TimeSeriesMiniCubeTest_map").put("executor", "1");
                runTest = true;
            }
        } finally {
            lock.unlock();
        }
        
        server.getCountDownLatch("TimeSeriesMiniCubeTest_cdl").trySetCount(1);
        
        if (!runTest) {
            System.out.println("Don't get the executor chance...");
            try {
                server.getCountDownLatch("TimeSeriesMiniCubeTest_cdl").await(10, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                // Ignore
            } finally {
                server.getCountDownLatch("TimeSeriesMiniCubeTest_cdl").countDown();
            }
            return;
        }
        
        try {
            DiscardListener.cdl.await();
            Thread.sleep(90 * 1000);
        } catch (InterruptedException e1) {
            // Ignore
        }
        
        int port = DiscardListener.applicationContext.getEmbeddedServletContainer().getPort();
        System.out.println("Run integration tests on " + port);
        
        int[] timeSerisArray = new int[] {20140606, 20140607, 20140608};
        StringBuilder sb = new StringBuilder();
        // Prepare time-series
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            String statusString = Unirest.get("http://localhost:" + port + "/status").asString().getBody();
            Map<String, List<String>> status = objectMapper.readValue(statusString, new TypeReference<Map<String, List<String>>>() {});
            for (int i = 0; i < status.get("awaiting").size(); i++) {
                // Assign role
                String assignResult = Unirest.post("http://localhost:" + port + "/reassign").header("accept", "application/json")
                    .field("cubeId", status.get("awaiting").get(i))
                    .field("timeSeries", timeSerisArray[i]).asString().getBody();
                System.out.println("Prepare time-series cube " + assignResult);
                sb.append("," + timeSerisArray[i]);
            }
        } catch (Exception e) {
            Assert.fail();
        }
        String timeSeriesString = sb.substring(1);
        System.out.println("Prepared time-series " + timeSeriesString);
        int actualTimeSeriesCount = timeSeriesString.split(",").length;
        // Sum
        try {
            String sumResult = Unirest.post("http://localhost:" + port + "/sum").header("accept", "application/json")
                .field("timeSeries", timeSeriesString)
                .field("indName", "CSM")
                .asString().getBody();
            Assert.assertEquals(actualTimeSeriesCount == 1 ? "305.14000000" : 
                (actualTimeSeriesCount == 2 ? "413.16000000" : 
                    (actualTimeSeriesCount == 3 ? "582.76000000" : "")), sumResult);
        } catch (Exception e) {
            Assert.fail();
        }
        
        // Merge
        try {
            String dummyMerge = Unirest.post("http://localhost:" + port + "/dummyMerge").header("accept", "application/json")
                    .field("timeSeries", 20140606)
                    .field("sql", "insert into minicube values(20140606,1023,1,51631,100.18000000,29.47000000,9,8836,1);")
                    .asString().getBody();
            Assert.assertEquals("ok", dummyMerge);
            
            String mergeResult = Unirest.post("http://localhost:" + port + "/merge").header("accept", "application/json")
                .field("timeSeries", 20140606)
                .field("version", "1")
                .asString().getBody();
            Assert.assertEquals("ok", mergeResult);
            
            String sumResult = Unirest.post("http://localhost:" + port + "/sum").header("accept", "application/json")
                    .field("timeSeries", timeSeriesString)
                    .field("indName", "CSM")
                    .asString().getBody();
            Assert.assertEquals(actualTimeSeriesCount == 1 ? "405.32000000" : 
                (actualTimeSeriesCount == 2 ? "513.34000000" : 
                    (actualTimeSeriesCount == 3 ? "682.94000000" : "")), sumResult);
        } catch (Exception e) {
            Assert.fail();
        }
        
    }
    
    @After
    public void shutDown() {
        HazelcastInstance server = DiscardListener.applicationContext.getBean(HazelcastInstance.class);
        server.getCountDownLatch("TimeSeriesMiniCubeTest_cdl").countDown();
    }

}
