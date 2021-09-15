package com.chinaums.canal.connector.direct.producer;

import com.alibaba.otter.canal.client.adapter.rdb.RdbAdapter;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.OuterAdapterConfig;
import com.alibaba.otter.canal.common.utils.PropertiesUtils;
import com.alibaba.otter.canal.connector.core.config.MQProperties;
import com.alibaba.otter.canal.connector.core.consumer.CommonMessage;
import com.alibaba.otter.canal.connector.core.producer.MQDestination;
import com.alibaba.otter.canal.connector.core.spi.CanalMQProducer;
import com.alibaba.otter.canal.connector.core.spi.SPI;
import com.alibaba.otter.canal.connector.core.util.Callback;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Direct Producer SPI 实现
 *
 * @author leonirvanel
 * @version 1.0.0
 */
@SPI("direct")
public class CanalDirectProducer implements CanalMQProducer {

    private static final Logger logger = LoggerFactory.getLogger(CanalDirectProducer.class);
    private MQProperties mqProperties = new MQProperties();
    private RdbAdapter rdbAdapter;
    private int sqlExceptionSleepSeconds = 3;

    @Override
    public void init(Properties properties) {
        Map<String, String> config = new HashMap<>();
        config.put("jdbc.driverClassName", PropertiesUtils.getProperty(properties, "direct.jdbc.driverClassName"));
        config.put("jdbc.url", PropertiesUtils.getProperty(properties, "direct.jdbc.url"));
        config.put("jdbc.username", PropertiesUtils.getProperty(properties, "direct.jdbc.username"));
        config.put("jdbc.password", PropertiesUtils.getProperty(properties, "direct.jdbc.password"));
        OuterAdapterConfig outerAdapterConfig = new OuterAdapterConfig();
        outerAdapterConfig.setProperties(config);
        rdbAdapter = new RdbAdapter();
        rdbAdapter.init(outerAdapterConfig, properties);
    }

    @Override
    public MQProperties getMqProperties() {
        return mqProperties;
    }

    @Override
    public void send(MQDestination destination, com.alibaba.otter.canal.protocol.Message message, Callback callback) {
        if (logger.isDebugEnabled()) {
            logger.debug("直接投递{}数据\r\n{}", destination.getCanalDestination(), message);
        }

        Throwable exception = null;
        try {
            if (CollectionUtils.isEmpty(message.getEntries())) {
                return;
            }

            List<CommonMessage> commonMessages = com.alibaba.otter.canal.connector.core.util.MessageUtil.convert(message);
            if (CollectionUtils.isEmpty(commonMessages)) {
                return;
            }

            List<Dml> dmls = com.alibaba.otter.canal.client.adapter.support.MessageUtil.flatMessage2Dml(destination.getCanalDestination(), "", commonMessages);
            rdbAdapter.sync(dmls);
        } catch (Throwable e) {
            logger.error("直接投递{}出错", destination.getCanalDestination(), ExceptionUtils.getRootCause(e));
            exception = e;
        } finally {
            if (null == exception) {
                callback.commit();
            } else {
                callback.rollback();
                try {
                    TimeUnit.SECONDS.sleep(sqlExceptionSleepSeconds);
                } catch (InterruptedException e) {
                }
            }
        }
    }

    @Override
    public void stop() {
        logger.info("## Stop Direct producer##");
        if (null != rdbAdapter) {
            rdbAdapter.destroy();
        }
    }
}
