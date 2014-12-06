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

import java.io.IOException;

import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.scene.Scene;
import javafx.scene.layout.AnchorPane;
import javafx.stage.Stage;

/**
 * Main class.
 * @author mengran
 *
 */
public class ClientApp extends Application {
    
    private Stage stage;
    
    @Override
    public void start(Stage primaryStage) throws Exception {
        
        this.stage = primaryStage;
        
        showCpuIdel();
        
        primaryStage.show();
    }
    
    private void showCpuIdel() {
        
        String cpuIdel = "cpuIdel.fxml";
        FXMLLoader loader = new FXMLLoader();
        loader.setLocation(ClientApp.class.getResource(cpuIdel));
        
        try {
            AnchorPane pane = (AnchorPane) loader.load();
            Scene scene = new Scene(pane);
            stage.setScene(scene);
            
            stage.setTitle("CPU Idel - Performance Testing");
        } catch (IOException e) {
            throw new RuntimeException("Fail to load fxml resource " + cpuIdel, e);
        }
        
    }
    
    public static void main(String[] args) {
        // Entry point if not javafx launcher
        launch(args);
    }
    
}
