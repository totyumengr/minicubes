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
package com.github.totyumengr.minicubes.fxclient;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javafx.application.Platform;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.scene.chart.CategoryAxis;
import javafx.scene.chart.LineChart;
import javafx.scene.chart.NumberAxis;
import javafx.scene.chart.XYChart;
import javafx.scene.chart.XYChart.Series;
import javafx.scene.control.Button;
import javafx.scene.control.Slider;
import javafx.scene.control.TextArea;

/**
 * CPU-idel controller class
 * @author mengran
 *
 */
public class CpuIdelController {
    
    @FXML
    private TextArea url;
    @FXML
    private Slider timeRange;
    @FXML
    private Slider threadRange;
    @FXML
    private Button start;
    @FXML
    private LineChart<String, Integer> cpuIdelLineChart;
    @FXML
    private CategoryAxis cpuIdelLineChartxAxis;
    @FXML
    private NumberAxis cpuIdelLineChartyAxis;
    
    private ScheduledExecutorService fetchCpuIdelData = new ScheduledThreadPoolExecutor(1);
    private ScheduledExecutorService redrawCpuIdelChart = new ScheduledThreadPoolExecutor(1);
    
    private LinkedBlockingQueue<Integer> cpuIdelDataQueue = new LinkedBlockingQueue<>();
    
    private class CpuIdelCollector implements Runnable {
        
        @Override
        public void run() {
            int y = getYAxis();
            try {
                cpuIdelDataQueue.offer(y, 500, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                // Ignore
            }
            System.out.println("insert a point to queue " + y);
        }
        
    }
    
    private Integer getYAxis() {
        
        return new Random().nextInt(100);
    }
    
    @FXML
    private void initialize() {
        
        cpuIdelLineChart.getData().add(new Series<String, Integer>());
        
        // Start to fetch data for line chart...
        fetchCpuIdelData.scheduleAtFixedRate(new CpuIdelCollector(), 100, 500, TimeUnit.MILLISECONDS);
        
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("mm:ss");
        // Start to redraw line chart...
        Runnable t = new Runnable() {
            
            @Override
            public void run() {
                Integer cpuIdel = -1;
                try {
                    cpuIdel = cpuIdelDataQueue.poll(500, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    // Ignore
                }
                if (cpuIdel >= 0) {
                    try {
                        ObservableList<XYChart.Data<String,Integer>> seriesData = cpuIdelLineChart.getData().get(0).getData();
                        seriesData.add(new XYChart.Data<String, Integer>(formatter.format(LocalDateTime.now()), cpuIdel));
                        cpuIdelLineChart.getData().get(0).setData(seriesData);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        };
        redrawCpuIdelChart.scheduleAtFixedRate(new Runnable() {
            
            @Override
            public void run() {
                Platform.runLater(t);
            }
        }, 1, 1, TimeUnit.SECONDS);
        
        System.out.println("Working...");
    }
    
    @FXML
    private void startOnAction() {
        
    }
    
    @FXML
    private void endOnAction() {
        
    }
}
