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

import java.util.concurrent.CountDownLatch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.SpringApplicationRunListener;
import org.springframework.boot.context.embedded.EmbeddedWebApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;

public class DiscardListener implements SpringApplicationRunListener {

    static EmbeddedWebApplicationContext applicationContext;
    static CountDownLatch cdl = new CountDownLatch(1);
    
    /**
     * 
     */
    public DiscardListener(SpringApplication application, String[] args) {
        super();
    }

    @Override
    public void started() {
        
    }

    @Override
    public void environmentPrepared(ConfigurableEnvironment environment) {
        
    }

    @Override
    public void contextPrepared(ConfigurableApplicationContext context) {
        
    }

    @Override
    public void contextLoaded(ConfigurableApplicationContext context) {
        
    }

    @Override
    public void finished(ConfigurableApplicationContext context,
            Throwable exception) {
        
        applicationContext = (EmbeddedWebApplicationContext) context;
        cdl.countDown();
    }
    
}
