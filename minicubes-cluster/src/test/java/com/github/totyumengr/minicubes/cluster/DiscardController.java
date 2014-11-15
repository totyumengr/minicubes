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

import javax.sql.DataSource;

import org.hibernate.validator.constraints.NotBlank;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * @author mengran
 *
 */
@Controller
public class DiscardController {

    private static final Logger LOGGER = LoggerFactory.getLogger(DiscardController.class);
    
    public static final String OK = "ok";
    
    /**
     * Only for dummy method
     */
    @Autowired
    private DataSource dataSource;
    
    @RequestMapping(value="/dummyMerge", method={RequestMethod.POST, RequestMethod.GET})
    public @ResponseBody String mergePrepare(@NotBlank @RequestParam String timeSeries, @RequestParam String sql) {
        
        LOGGER.info("Try to merge data to {}.", sql, timeSeries);
        JdbcTemplate template = new JdbcTemplate(dataSource);
        template.afterPropertiesSet();
        template.execute(sql);
        LOGGER.info("Success for merge data to {}.", sql, timeSeries);
        
        return OK;
    }
}
