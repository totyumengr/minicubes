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

import javafx.application.Application;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.layout.StackPane;
import javafx.stage.Stage;

/**
 * Main class.
 * @author mengran
 *
 */
public class ClientApp extends Application {
    
    @Override
    public void start(Stage primaryStage) throws Exception {
        
        // Create pane
        StackPane root = new StackPane();
        // Create scene
        Scene scene = new Scene(root, 300, 400);
        
        // Build pane
        build(root);
        
        // Mount to stage
        primaryStage.setScene(scene);
        primaryStage.setTitle("Hello minicubes-fxclient");
        
        primaryStage.show();
    }
    
    private void build(StackPane root) {
        
        // FIXME: Change to actual client.
        
        Button hello = new Button("Click me!");
        hello.setOnAction(new EventHandler<ActionEvent>() {
            
            @Override
            public void handle(ActionEvent event) {
                System.out.println("Clicked...");
            }
        });
        
        root.getChildren().add(hello);
    }
    
    public static void main(String[] args) {
        // Entry point if not javafx launcher
        launch(args);
    }
    
}
