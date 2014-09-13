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

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import md.math.DoubleDouble;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.SqlParameterValue;
import org.springframework.jdbc.core.SqlTypeValue;
import org.springframework.jdbc.core.StatementCreatorUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import com.github.totyumengr.minicubes.core.Aggregations;
import com.github.totyumengr.minicubes.core.FactTable.FactTableBuilder;
import com.github.totyumengr.minicubes.core.MiniCube;
import com.hazelcast.config.Config;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.ManagementCenterConfig;
import com.hazelcast.config.MulticastConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.core.MultiExecutionCallback;

/**
 * Implementation beyond {@link Hazelcast}.
 * 
 * <p>{@link #factSourceSql} is important, we use the {@link ResultSetMetaData#getColumnLabel(int)} as meta data.
 * Please make sure to put dimension in <b>front</b> of measure, and you should classify the split 
 * point by {@link #splitIndex index from 0}. On the other let it equal <code>-1</code> then we try to split by meta data:
 * start with <b>dim_</b>, we use it as dimension.
 * 
 * <p>{@link #factSourceSql} must have a <code>?</code> so we will specify this when {@link #reassignRole(String, String)}
 * @author mengran
 *
 */
@Service
@Configuration
public class TimeSeriesMiniCubeManagerHzImpl implements TimeSeriesMiniCubeManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(TimeSeriesMiniCubeManagerHzImpl.class);
    
    private static final String DISTRIBUTED_EXECUTOR = "distributedExecutor";
    
    private static final String MINICUBE_MANAGER = "minicubeManager";
    
    private static final ThreadLocal<String[]> AGG_CONTEXT = new ThreadLocal<String[]>();
    
    @Autowired
    private Environment env;
    
    @Autowired
    private HazelcastInstance hazelcastInstance;
    
    private String hzGroupName;
    @Value("${hazelcast.executor.timeout}")
    private int hzExecutorTimeout;
    
    @Autowired
    private DataSource dataSource;
    @Value("${minicube.builder.sourceSql}")
    private String factSourceSql;
    @Value("${minicube.measure.fromIndex}")
    private int splitIndex = -1;
    
    /**
     * Manage target object.
     */
    private transient MiniCube miniCube;
    
    @Bean
    public HazelcastInstance hazelcastServer() {
        
        Config hazelcastConfig = new Config("minicubes-cluster");
        hazelcastConfig.setProperty("hazelcast.system.log.enabled", "false");
        hazelcastConfig.setProperty("hazelcast.logging.type", "slf4j");
        
        hazelcastConfig.setGroupConfig(new GroupConfig(hzGroupName = env.getRequiredProperty("hazelcast.group.name"), 
                env.getRequiredProperty("hazelcast.group.password")));
        if (StringUtils.hasText(env.getRequiredProperty("hazelcast.mancenter.url"))) {
            hazelcastConfig.setManagementCenterConfig(new ManagementCenterConfig()
                .setEnabled(true)
                .setUrl(env.getRequiredProperty("hazelcast.mancenter.url")));
        }
        String hzMembers;
        JoinConfig joinConfig = new JoinConfig();
        if (!StringUtils.hasText(hzMembers = env.getRequiredProperty("hazelcast.members"))) {
            // Means multiple cast
            joinConfig.setMulticastConfig(new MulticastConfig().setEnabled(true));
        } else {
            joinConfig.setMulticastConfig(new MulticastConfig().setEnabled(false))
                .setTcpIpConfig(new TcpIpConfig().setEnabled(true).addMember(hzMembers));
        }
        hazelcastConfig.setNetworkConfig(new NetworkConfig().setJoin(joinConfig));
        
        // New ExecutorService
        int hzExecutorSize = -1;
        if ((hzExecutorSize = env.getRequiredProperty("hazelcast.executor.poolsize", Integer.class)) < 0) {
            hzExecutorSize = Runtime.getRuntime().availableProcessors();
            LOGGER.info("Use cpu core count {} setting into executor service", hzExecutorSize);
        }
        hazelcastConfig.addExecutorConfig(new ExecutorConfig(DISTRIBUTED_EXECUTOR, hzExecutorSize)
            .setQueueCapacity(env.getRequiredProperty("hazelcast.executor.queuecapacity", Integer.class)));
        
        // Add member event listener
        hazelcastConfig.addListenerConfig(new ListenerConfig().setImplementation(new MembershipListener() {
            
            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {
                // Mean a member leave out of cluster.
                LOGGER.info("A member {} has removed from cluster {}", 
                        membershipEvent.getMember(), membershipEvent.getCluster().getClusterTime());
                
                IMap<String, String> miniCubeManager = hazelcastInstance.getMap(MINICUBE_MANAGER);
                LOGGER.info("Minicube manager status {}", ObjectUtils.getDisplayString(miniCubeManager));
                
                // FIXME: Schedule to remove relationship after "long disconnect".
            }
            
            @Override
            public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {
                // Let it empty.
            }
            
            @Override
            public void memberAdded(MembershipEvent membershipEvent) {
                // Mean a member join into cluster.
                LOGGER.info("A member {} has joined into cluster {}, let it's self to claim a role.", 
                        membershipEvent.getMember(), membershipEvent.getCluster().getClusterTime());
            }
        }));
        
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(hazelcastConfig);
        // Put execute context
        instance.getUserContext().put("this", TimeSeriesMiniCubeManagerHzImpl.this);
        
        // Handle new member
        handleNewMember(instance, instance.getCluster().getLocalMember());
        
        return instance;
    }
    
    private void handleNewMember(HazelcastInstance instance, Member member) {
        
        // Relationship between Member and MiniCube ID
        IMap<String, String> miniCubeManager = instance.getMap(MINICUBE_MANAGER);
        LOGGER.info("Minicube manager status {}", ObjectUtils.getDisplayString(miniCubeManager));
        
        String key = member.getSocketAddress().toString();
        ILock lock = instance.getLock("miniCubeLock");
        lock.lock();
        try {
            if (miniCubeManager.containsKey(key) && !miniCubeManager.get(key).startsWith("?")) {
                // Maybe node-restart
                LOGGER.info("A node{} restarted, so we need rebuild cube{}", key, miniCubeManager.get(key));
                // Reassign task.
                reassignRole(miniCubeManager.get(key), miniCubeManager.get(key).split("::")[0]);
            } else {
                // First time join into cluster
                String id = "?" + "::" + hzGroupName + "@" + key;
                miniCubeManager.put(key, id);
                
                member.setStringAttribute("cubeId", id);
                LOGGER.info("Add {} into cluster {}", id, hzGroupName);
            }
        } finally {
            lock.unlock();
        }
    }
    
    // ------------------------------ Implementation ------------------------------

    @Override
    public <T> List<T> execute(Callable<T> task, Collection<String> cubeIds, int timeoutSeconds) {
        
        Set<Member> members = hazelcastInstance.getCluster().getMembers();
        Set<Member> selected = new LinkedHashSet<Member>();
        if (cubeIds != null && !cubeIds.isEmpty()) {
            List<String> cubeNodes = cubeIds.stream().map(e -> e.split("@")[1]).collect(Collectors.toList());
            for (Member m : members) {
                if (cubeNodes.contains(m.getSocketAddress().toString())) {
                    selected.add(m);
                }
            }
        } else {
            selected.addAll(members);
            LOGGER.warn("Select all members {} in cluster to execute on.", selected);
        }
        
        final int size = selected.size();
        LOGGER.debug("Start to run task {} on {}", task, selected);
        
        // Call distributed execute service to run it.
        final List<T> result = new ArrayList<T>(selected.size());
        final List<Exception> exceptionResult = new ArrayList<Exception>();
        CountDownLatch cdl = new CountDownLatch(1);
        AtomicInteger completedCount = new AtomicInteger(0);
        hazelcastInstance.getExecutorService(DISTRIBUTED_EXECUTOR).submitToMembers(task, selected,
                new MultiExecutionCallback() {
                    
                    @SuppressWarnings("unchecked")
                    @Override
                    public void onResponse(Member member, Object value) {
                        int i = completedCount.incrementAndGet();
                        LOGGER.debug("Completed of {}/{}, {} and {}", i, size, member, value);
                        if (value instanceof Exception) {
                            exceptionResult.add((Exception) value);
                        } else {
                            result.add((T) value);
                        }
                    }
                    
                    @Override
                    public void onComplete(Map<Member, Object> values) {
                        LOGGER.info("Successfully execute {} on cluster, collect {} result.", task, values.size());
                        cdl.countDown();
                    }
                });
        
        if (completedCount.get() < size) {
            // FIXME: When some task do not executed. Maybe reject? error?
        }
        
        try {
            cdl.await(timeoutSeconds > 0 ? timeoutSeconds : Integer.MAX_VALUE, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // Ignore
        }
        
        // Exception handled
        if (!exceptionResult.isEmpty()) {
            LOGGER.error("{} exceptions occurred when try to execute {} on {}", exceptionResult.size(), task, 
                    ObjectUtils.getDisplayString(selected));
            for (int i = 0; i < exceptionResult.size(); i++) {
                LOGGER.error("#1 exception === ", exceptionResult.get(i));
            }
            throw new RuntimeException("Exception occurred when try to execute, please see detail logs above.");
        }
        
        return result;
    }
    
    private static class Assign implements Callable<String>, HazelcastInstanceAware, Serializable {

        /**
         * 
         */
        private static final long serialVersionUID = 1L;
        
        private transient HazelcastInstance instance;
        private transient TimeSeriesMiniCubeManagerHzImpl impl;
        
        private String cubeId;
        private String timeSeries;
        
        public Assign(String cubeId, String timeSeries) {
            super();
            this.cubeId = cubeId;
            this.timeSeries = timeSeries;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            instance = hazelcastInstance;
            impl = (TimeSeriesMiniCubeManagerHzImpl) instance.getUserContext().get("this");
        }

        @Override
        public String call() throws Exception {
            Member localMember = instance.getCluster().getLocalMember();
            String member = cubeId.split("@")[1];
            if (!member.equals(localMember.getSocketAddress().toString())) {
                throw new UnsupportedOperationException("Assignment " + cubeId + " " + timeSeries
                        + "only permitted to run in local member.");
            }
            
            LOGGER.info("Start fetching data and building cube {}, {}", cubeId, timeSeries);
            
            JdbcTemplate template = new JdbcTemplate(impl.dataSource);
            FactTableBuilder builder = new FactTableBuilder();
            boolean builded = false;
            AtomicInteger rowCount = new AtomicInteger();
            try {
                builder.build(timeSeries, impl.splitIndex - 1);
                AtomicBoolean processMeta = new AtomicBoolean(true);
                AtomicInteger actualSplitIndex = new AtomicInteger();
                
                List<SqlParameterValue> params = new ArrayList<SqlParameterValue>();
                if (timeSeries.length() == 8) {
                    SqlParameterValue v = new SqlParameterValue(SqlTypeValue.TYPE_UNKNOWN, timeSeries);
                    params.add(v);
                } else if (timeSeries.length() == 6) {
                    SqlParameterValue start = new SqlParameterValue(SqlTypeValue.TYPE_UNKNOWN, timeSeries + "01");
                    SqlParameterValue end = new SqlParameterValue(SqlTypeValue.TYPE_UNKNOWN, timeSeries + "31");
                    params.add(start);
                    params.add(end);
                } else {
                    throw new IllegalArgumentException("Only supported day or month format." + timeSeries);
                }
                template.query(new PreparedStatementCreator() {
                    
                        @Override
                        public PreparedStatement createPreparedStatement(Connection con)
                                throws SQLException {
                            
                            PreparedStatement stmt = con.prepareStatement(impl.factSourceSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                            // MySQL steaming result-set http://dev.mysql.com/doc/connector-j/en/connector-j-reference-implementation-notes.html
                            // http://stackoverflow.com/questions/2095490/how-to-manage-a-large-dataset-using-spring-mysql-and-rowcallbackhandler
                            try {
                                stmt.setFetchSize(Integer.MIN_VALUE);
                                LOGGER.info("Set stream feature of MySQL. {}", stmt.getFetchSize());
                            } catch (Exception e) {
                                // Ignore maybe do not supported.
                            }
                            for (int i = 0; i < params.size(); i++) {
                                StatementCreatorUtils.setParameterValue(stmt, i + 1, params.get(i), params.get(i).getValue());
                            }
                            return stmt;
                        }
                    }, new RowCallbackHandler() {
    
                        @Override
                        public void processRow(ResultSet rs) throws SQLException {
                            if (processMeta.get()) {
                                ResultSetMetaData meta = rs.getMetaData();
                                int dimSize = 0;
                                if (impl.splitIndex < 0) {
                                    LOGGER.debug("Not specify splitIndex so we guess by column labels");
                                    for (int i = 1; i <= meta.getColumnCount(); i++) {
                                        if (meta.getColumnLabel(i).toLowerCase().startsWith("dim_")) {
                                            LOGGER.debug("Add dim column {}", meta.getColumnLabel(i));
                                            builder.addDimColumns(Arrays.asList(new String[] {meta.getColumnLabel(i)}));
                                            dimSize++;
                                        } else {
                                            LOGGER.debug("Add measure column {}", meta.getColumnLabel(i));
                                            builder.addIndColumns(Arrays.asList(new String[] {meta.getColumnLabel(i)}));
                                        }
                                    }
                                } else {
                                    LOGGER.debug("Specify splitIndex {} means measure start.");
                                    for (int i = 1; i <= meta.getColumnCount(); i++) {
                                        if (i < impl.splitIndex) {
                                            LOGGER.debug("Add dim column {}", meta.getColumnLabel(i));
                                            builder.addDimColumns(Arrays.asList(new String[] {meta.getColumnLabel(i)}));
                                            dimSize++;
                                        } else {
                                            LOGGER.debug("Add measure column {}", meta.getColumnLabel(i));
                                            builder.addIndColumns(Arrays.asList(new String[] {meta.getColumnLabel(i)}));
                                        }
                                    }
                                }
                                actualSplitIndex.set(dimSize);
                                // End meta setting
                                processMeta.set(false);
                            }
                            
                            // Add fact data
                            List<Long> dimDatas = new ArrayList<Long>(actualSplitIndex.get());
                            List<DoubleDouble> indDatas = new ArrayList<DoubleDouble>(rs.getMetaData().getColumnCount() - dimDatas.size());
                            for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
                                if (i < actualSplitIndex.get()) {
                                    dimDatas.add(rs.getLong(i + 1));
                                } else {
                                    indDatas.add(DoubleDouble.valueOf(rs.getDouble(i + 1)));
                                }
                            }
                            rowCount.incrementAndGet();
                            builder.addDimDatas(rowCount.get(), dimDatas);
                            builder.addIndDatas(rowCount.get(), indDatas);
                            
                            if (rowCount.get() % 1000000 == 0) {
                                LOGGER.info("Loaded {} records into cube.", rowCount.get());
                            }
                        }
                    }
                );
                
                // Ending build operation
                impl.miniCube = new MiniCube(builder.done());
                builded = true;
                
                String newCubeId = timeSeries + "::" + impl.hzGroupName + "@" + member;
                LOGGER.info("Success to build cube {} from {} and {}", newCubeId, cubeId, timeSeries);
                
                // Put relationship into member
                localMember.setStringAttribute("cubeId", newCubeId);
                IMap<String, String> miniCubeManager = instance.getMap(MINICUBE_MANAGER);
                miniCubeManager.put(member, newCubeId);
                
                return newCubeId;
            } finally {
                if (!builded) {
                    builder.done();
                }
            }
        }
        
    }

    @Override
    public String reassignRole(String cubeId, String timeSeries) {
        
        LOGGER.info("Starting to assign {} to calculating {} calculation.", cubeId, timeSeries);
        
        // FIXME: Validation with Spring MVC, so we do not double-check it. Enhancement in future version.
        
        if (cubeId.startsWith("?")) {
            LOGGER.debug("First time assign {}", cubeId);
        }
        // Do it in self VM
        List<String> result = execute(new Assign(cubeId, timeSeries), Arrays.asList(new String[] {cubeId}), -1);
        Assert.hasText(result.get(0), "Fail to assign " + cubeId + " " + timeSeries);
        
        LOGGER.info("Successfully reassign role {}", result.get(0));
        return result.get(0);
    }
    
    @Override
    public Collection<String> allCubeIds() {
        
        Set<Member> members = hazelcastInstance.getCluster().getMembers();
        // Exact match
        return members.stream().filter(e -> e.getStringAttribute("cubeId") != null)
                .map(e -> e.getStringAttribute("cubeId")).collect(Collectors.toList());
    }

    @Override
    public Collection<String> cubeIds(String cubeDate) {
        
        Set<Member> members = hazelcastInstance.getCluster().getMembers();
        // Exact match
        return members.stream().filter(e -> e.getStringAttribute("cubeId").split("::")[0].startsWith(cubeDate))
                .map(e -> e.getStringAttribute("cubeId")).collect(Collectors.toList());
    }
    
    @Override
    public Aggregations aggs(String... timeSeries) {
        
        AGG_CONTEXT.set(timeSeries);
        return this;
    }
    
    private static class Sum implements Callable<BigDecimal>, HazelcastInstanceAware, Serializable {

        /**
         * 
         */
        private static final long serialVersionUID = 1L;
        
        private transient HazelcastInstance instance;
        private transient TimeSeriesMiniCubeManagerHzImpl impl;
        
        private String indName;
        private Map<String, List<Long>> filterDims;
        
        public Sum(String indName, Map<String, List<Long>> filterDims) {
            super();
            this.indName = indName;
            this.filterDims = filterDims;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.instance = hazelcastInstance;
            impl = (TimeSeriesMiniCubeManagerHzImpl) instance.getUserContext().get("this");
        }

        @Override
        public BigDecimal call() throws Exception {
            
            LOGGER.info("Sum on {}", instance.getCluster().getLocalMember());
            return impl.miniCube == null ? new BigDecimal(0) : impl.miniCube.sum(indName, filterDims);
        }
        
    }

    @Override
    public BigDecimal sum(String indName) {
        
        return sum(indName, null);
    }

    @Override
    public BigDecimal sum(String indName, Map<String, List<Long>> filterDims) {
        
        Set<String> cubeIds = new LinkedHashSet<String>();
        try {
            String[] timeSeries = AGG_CONTEXT.get();
            if (timeSeries == null || timeSeries.length == 0) {
                cubeIds.addAll(allCubeIds());
            } else {
                for (String t : timeSeries) {
                    cubeIds.addAll(cubeIds(t));
                }
                if (cubeIds.isEmpty()) {
                    throw new IllegalArgumentException("Can not find availd cubes for given time series "
                            + ObjectUtils.getDisplayString(timeSeries));
                }
            }
            LOGGER.info("Agg on cubes {}", ObjectUtils.getDisplayString(cubeIds));
            
            // Do execute
            List<BigDecimal> results = execute(new Sum(indName, filterDims), cubeIds, hzExecutorTimeout);
            LOGGER.info("Sum {} on {} results is {}", indName, cubeIds, results);
            
            return results.stream().reduce(new BigDecimal(0), (x, y) -> x.add(y))
                    .setScale(IND_SCALE, BigDecimal.ROUND_HALF_UP);
            
        } finally {
            AGG_CONTEXT.remove();
        }
    }
    
    /**
     * FIXME: Need re-design
     * @author mengran
     *
     */
    private static class Sum2 implements Callable<Map<Long, BigDecimal>>, HazelcastInstanceAware, Serializable {

        /**
         * 
         */
        private static final long serialVersionUID = 1L;
        
        private transient HazelcastInstance instance;
        private transient TimeSeriesMiniCubeManagerHzImpl impl;
        
        private String indName;
        private Map<String, List<Long>> filterDims;
        private String groupDimName;
        
        public Sum2(String indName, String groupDimName, Map<String, List<Long>> filterDims) {
            super();
            this.indName = indName;
            this.filterDims = filterDims;
            this.groupDimName = groupDimName;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.instance = hazelcastInstance;
            impl = (TimeSeriesMiniCubeManagerHzImpl) instance.getUserContext().get("this");
        }

        @Override
        public Map<Long, BigDecimal> call() throws Exception {
            
            LOGGER.info("Sum on {}", instance.getCluster().getLocalMember());
            return impl.miniCube == null ? null : impl.miniCube.sum(indName, groupDimName, filterDims);
        }
        
    }

    @Override
    public Map<Long, BigDecimal> sum(String indName, String groupByDimName,
            Map<String, List<Long>> filterDims) {
        
        Set<String> cubeIds = new LinkedHashSet<String>();
        try {
            String[] timeSeries = AGG_CONTEXT.get();
            if (timeSeries == null || timeSeries.length == 0) {
                cubeIds.addAll(allCubeIds());
            } else {
                for (String t : timeSeries) {
                    cubeIds.addAll(cubeIds(t));
                }
                if (cubeIds.isEmpty()) {
                    throw new IllegalArgumentException("Can not find availd cubes for given time series "
                            + ObjectUtils.getDisplayString(timeSeries));
                }
            }
            LOGGER.info("Agg on cubes {}", ObjectUtils.getDisplayString(cubeIds));
            
            // Do execute
            List<Map<Long, BigDecimal>> results = execute(new Sum2(indName, groupByDimName, filterDims), 
                    cubeIds, hzExecutorTimeout);
            LOGGER.debug("Group {} on {} with filter {} results is {}", indName, cubeIds, filterDims, results);
            
            Map<Long, BigDecimal> result = new HashMap<Long, BigDecimal>();
            results.stream().forEach(new Consumer<Map<Long, BigDecimal>>() {

                @Override
                public void accept(Map<Long, BigDecimal> t) {
                    t.forEach((k, v) -> result.merge(k, v, BigDecimal::add));
                }
            });
            LOGGER.info("Sum {} on {} with filter {} results is {}", indName, cubeIds, filterDims, result);
            
            return result;
        } finally {
            AGG_CONTEXT.remove();
        }
    }
    
}
