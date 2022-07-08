/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.registry.client.migration;

import org.apache.dubbo.common.config.ConfigurationUtils;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.registry.client.migration.model.MigrationRule;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.cluster.ClusterInvoker;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultMigrationAddressComparator implements MigrationAddressComparator {
    private static final Logger logger = LoggerFactory.getLogger(DefaultMigrationAddressComparator.class);
    private static final String MIGRATION_THRESHOLD = "dubbo.application.migration.threshold";
    private static final String DEFAULT_THRESHOLD_STRING = "0.0";
    private static final float DEFAULT_THREAD = 0f;

    public static final String OLD_ADDRESS_SIZE = "OLD_ADDRESS_SIZE";
    public static final String NEW_ADDRESS_SIZE = "NEW_ADDRESS_SIZE";

    private Map<String, Map<String, Integer>> serviceMigrationData = new ConcurrentHashMap<>();

    @Override
    public <T> boolean shouldMigrate(ClusterInvoker<T> newInvoker, ClusterInvoker<T> oldInvoker, MigrationRule rule) {
        // newInvoker指的是应用级Invoker，oldInvoker指的是接口级Invoker

        Map<String, Integer> migrationData = serviceMigrationData.computeIfAbsent(oldInvoker.getUrl().getDisplayServiceKey(), _k -> new ConcurrentHashMap<>());

        // 没有应用级Invoker，return false，表示使用接口级Invoker
        if (!newInvoker.hasProxyInvokers()) {
            migrationData.put(OLD_ADDRESS_SIZE, getAddressSize(oldInvoker));
            migrationData.put(NEW_ADDRESS_SIZE, -1);
            logger.info("No " + getInvokerType(newInvoker) + " address available, stop compare.");
            return false;
        }

        // 没有接口级Invoker，return true，表示使用应用级Invoker
        if (!oldInvoker.hasProxyInvokers()) {
            migrationData.put(OLD_ADDRESS_SIZE, -1);
            migrationData.put(NEW_ADDRESS_SIZE, getAddressSize(newInvoker));
            logger.info("No " + getInvokerType(oldInvoker) + " address available, stop compare.");
            return true;
        }

        // 正常来说这两种级别的Invoker数应该是一样的
        // 但是应用如果有两个实例，一个实例用的是2.7（只能进行接口级注册），一个用的是3.0（接口级和应用级都进行了注册）
        // 那么接口级Invoker就会比应用级Invoker多
        int newAddressSize = getAddressSize(newInvoker); // 应用级Invoker的数量  tri dubbo rest
        int oldAddressSize = getAddressSize(oldInvoker); // 接口级Invoker的数量 rest dubbo

        migrationData.put(OLD_ADDRESS_SIZE, oldAddressSize);
        migrationData.put(NEW_ADDRESS_SIZE, newAddressSize);

        String rawThreshold = null;
        Float configuredThreshold = rule == null ? null : rule.getThreshold(oldInvoker.getUrl());
        if (configuredThreshold != null && configuredThreshold >= 0) {
            rawThreshold = String.valueOf(configuredThreshold);
        }
        // 先从迁移规则中获取rawThreshold，如果没有配，则获取dubbo.application.migration.threshold配置的值，如果也没有配默认就0
        rawThreshold = StringUtils.isNotEmpty(rawThreshold) ? rawThreshold : ConfigurationUtils.getCachedDynamicProperty(newInvoker.getUrl().getScopeModel(), MIGRATION_THRESHOLD, DEFAULT_THRESHOLD_STRING);
        float threshold;
        try {
            threshold = Float.parseFloat(rawThreshold);
        } catch (Exception e) {
            logger.error("Invalid migration threshold " + rawThreshold);
            threshold = DEFAULT_THREAD;
        }

        logger.info("serviceKey:" + oldInvoker.getUrl().getServiceKey() + " Instance address size " + newAddressSize + ", interface address size " + oldAddressSize + ", threshold " + threshold);
        // 只有应用级Invoker
        if (newAddressSize != 0 && oldAddressSize == 0) {
            return true;
        }
        // 都没有
        if (newAddressSize == 0 && oldAddressSize == 0) {
            return false;
        }

        // 既有接口级Invoker，又有应用级Invoker
        // threshold默认为0，
        if (((float) newAddressSize / (float) oldAddressSize) >= threshold) {
            return true;
        }
        return false;
    }

    private <T> int getAddressSize(ClusterInvoker<T> invoker) {
        if (invoker == null) {
            return -1;
        }
        List<Invoker<T>> invokers = invoker.getDirectory().getAllInvokers();
        return CollectionUtils.isNotEmpty(invokers) ? invokers.size() : 0;
    }

    public Map<String, Integer> getAddressSize(String displayServiceKey) {
        return serviceMigrationData.get(displayServiceKey);
    }

    private String getInvokerType(ClusterInvoker<?> invoker) {
        if (invoker.isServiceDiscovery()) {
            return "instance";
        }
        return "interface";
    }


}
