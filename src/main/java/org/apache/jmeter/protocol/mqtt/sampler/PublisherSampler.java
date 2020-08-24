/*
 * Copyright 2017 Hemika Yasinda Kodikara
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jmeter.protocol.mqtt.sampler;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.jmeter.protocol.mqtt.client.ClientPool;
import org.apache.jmeter.protocol.mqtt.paho.clients.AsyncClient;
import org.apache.jmeter.protocol.mqtt.paho.clients.BaseClient;
import org.apache.jmeter.protocol.mqtt.paho.clients.BlockingClient;
import org.apache.jmeter.protocol.mqtt.utilities.Constants;
import org.apache.jmeter.protocol.mqtt.utilities.Utils;
import org.apache.jmeter.samplers.AbstractSampler;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testelement.TestStateListener;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;
import org.eclipse.paho.client.mqttv3.MqttException;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This is MQTT Publisher sample class. The implementation includes publishing of MQTT messages with the sample
 * processing.
 */
public class PublisherSampler extends AbstractSampler implements TestStateListener {


    private transient BaseClient client;
    private String topicName = StringUtils.EMPTY;
    private boolean retained;
    private String messageInputType;
    private long timeout;

    private AtomicInteger publishedMessageCount = new AtomicInteger(0);
    private static final String nameLabel = "MQTT Publisher";
    private static final String lineSeparator = System.getProperty("line.separator");

    private static final long serialVersionUID = 233L;
    private static final Logger log = LoggingManager.getLoggerForClass();

    private static final String BROKER_URL = "mqtt.broker.url";
    private static final String CLIENT_ID = "mqtt.client.id";
    private static final String TOPIC_NAME = "mqtt.topic.name";
    private static final String RETAINED = "mqtt.message.retained";
    private static final String CLEAN_SESSION = "mqtt.clean.session";
    private static final String KEEP_ALIVE = "mqtt.keep.alive";
    private static final String PUBLISH_TIMEOUT = "mqtt.publish.timeout";
    private static final String USERNAME = "mqtt.auth.username";
    private static final String PASSWORD = "mqtt.auth.password";
    private static final String QOS = "mqtt.qos";
    private static final String CLIENT_TYPE = "mqtt.client.type";
    private static final String MESSAGE_INPUT_TYPE = "mqtt.message.input.type";
    private static final String MESSAGE_VALUE = "mqtt.message.input.value";

    // Getters
    public String getBrokerUrl() {
        return getPropertyAsString(BROKER_URL);
    }

    public String getClientId() {
        return getPropertyAsString(CLIENT_ID);
    }

    public String getTopicName() {
        return getPropertyAsString(TOPIC_NAME);
    }

    public boolean isMessageRetained() {
        return getPropertyAsBoolean(RETAINED);
    }

    public boolean isCleanSession() {
        return getPropertyAsBoolean(CLEAN_SESSION);
    }

    public int getKeepAlive() {
        return getPropertyAsInt(KEEP_ALIVE);
    }

    public int getPublishTimeout() {
        return getPropertyAsInt(PUBLISH_TIMEOUT);
    }

    public String getUsername() {
        return getPropertyAsString(USERNAME);
    }

    public String getPassword() {
        return getPropertyAsString(PASSWORD);
    }

    public String getQOS() {
        return getPropertyAsString(QOS);
    }

    public String getClientType() {
        return getPropertyAsString(CLIENT_TYPE);
    }

    public String getMessageInputType() {
        return getPropertyAsString(MESSAGE_INPUT_TYPE);
    }

    public String getMessageValue() {
        return getPropertyAsString(MESSAGE_VALUE);
    }

    private String getNameLabel() {
        return nameLabel;
    }

    // Setters
    public void setBrokerUrl(String brokerURL) {
        setProperty(BROKER_URL, brokerURL.trim());
    }

    public void setClientId(String clientID) {
        setProperty(CLIENT_ID, clientID.trim());
    }

    public void setTopicName(String topicName) {
        setProperty(TOPIC_NAME, topicName.trim());
    }

    public void setMessageRetained(boolean isCleanSession) {
        setProperty(RETAINED, isCleanSession);
    }

    public void setCleanSession(boolean isCleanSession) {
        setProperty(CLEAN_SESSION, isCleanSession);
    }

    public void setKeepAlive(String keepAlive) {
        setProperty(KEEP_ALIVE, keepAlive);
    }

    public void setPublishTimeout(String publishTimeout) {
        setProperty(PUBLISH_TIMEOUT, publishTimeout);
    }

    public void setUsername(String username) {
        setProperty(USERNAME, username.trim());
    }

    public void setPassword(String password) {
        setProperty(PASSWORD, password.trim());
    }

    public void setQOS(String qos) {
        setProperty(QOS, qos.trim());
    }

    public void setClientType(String clientType) {
        setProperty(CLIENT_TYPE, clientType.trim());
    }

    public void setMessageInputType(String messageInputType) {
        setProperty(MESSAGE_INPUT_TYPE, messageInputType.trim());
    }

    public void setMessageValue(String messageValue) {
        setProperty(MESSAGE_VALUE, messageValue.trim());
    }

    public PublisherSampler() {
    }

    @Override
    public void testStarted() {
    }

    @Override
    public void testStarted(String s) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void testEnded() {
        log.debug("Test ended, clearing client pool");
        try {
            ClientPool.clearClient();
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void testEnded(String arg0) {
        testEnded();
    }

    private void initClient() throws MqttException {
        try {
            String brokerURL = getBrokerUrl();
            String clientId = getClientId();
            boolean isCleanSession = isCleanSession();
            int keepAlive = getKeepAlive();
            String userName = getUsername();
            String password = getPassword();
            String clientType = getClientType();

            topicName = getTopicName();
            retained = isMessageRetained();
            messageInputType = getMessageInputType();
            timeout = getPublishTimeout();
            if (timeout > 0) {
                timeout = timeout * 1000L;
            }

            // Generating client ID if empty
            if (StringUtils.isEmpty(clientId)) {
                clientId = Utils.UUIDGenerator();
            }
            
            if (Constants.MQTT_BLOCKING_CLIENT.equals(clientType)) {
                client = new BlockingClient(brokerURL, clientId, isCleanSession, userName, password, keepAlive);
            } else if (Constants.MQTT_ASYNC_CLIENT.equals(clientType)) {
                client = new AsyncClient(brokerURL, clientId, isCleanSession, userName, password, keepAlive);
            }
            
            if (null != client) {
                ClientPool.addClient(client);
            }
        } catch (MqttException e) {
            log.error(getClientId() + ": " +   e.getMessage(), e);
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SampleResult sample(Entry entry) {
        SampleResult result = new SampleResult();
        result.sampleStart();
        if (client == null || !client.isConnected()) {
            try {
                initClient();
            } catch (MqttException e) {
                result.sampleEnd(); // stop stopwatch
                result.setSuccessful(false);
                // get stack trace as a String to return as document data
                java.io.StringWriter stringWriter = new java.io.StringWriter();
                e.printStackTrace(new java.io.PrintWriter(stringWriter));
                result.setResponseData(stringWriter.toString(), null);
                result.setResponseMessage("Unable publish messages." + lineSeparator + "Exception: " + e.toString());
                result.setDataType(org.apache.jmeter.samplers.SampleResult.TEXT);
                result.setResponseCode("FAILED");
                return result;
            }
        }
        result.setSampleLabel(getNameLabel() + "::" + getClientId());

        try {
            // Quality
            int qos;
            if (Constants.MQTT_AT_MOST_ONCE.equals(getQOS())) {
                qos = 0;
            } else if (Constants.MQTT_AT_LEAST_ONCE.equals(getQOS())) {
                qos = 1;
            } else if (Constants.MQTT_EXACTLY_ONCE.equals(getQOS())) {
                qos = 2;
            } else {
                qos = 0;
            }

            byte[] publishMessage;
            if (Constants.MQTT_MESSAGE_INPUT_TYPE_TEXT.equals(messageInputType)) {
                publishMessage = getMessageValue().getBytes();
            } else if (Constants.MQTT_MESSAGE_INPUT_TYPE_FILE.equals(messageInputType)) {
                //TODO how to handle if file not available (what happens @end of CSV
                String filename = getMessageValue();
                if (log.isDebugEnabled()) log.debug(getClientId() + " reading file: " + filename);
                publishMessage = FileUtils.readFileToByteArray(new File(filename));
            } else {
                publishMessage = new byte[0];
            }

            long durationNanos = client.publish(topicName, qos, publishMessage, retained, timeout);
            result.setSuccessful(true);
            result.setLatency(durationNanos / 1000000);
            result.setBytes(publishMessage.length);
            result.setBodySize(publishMessage.length);
            result.sampleEnd(); // stop stopwatch
            result.setResponseMessage("Sent " + publishedMessageCount.incrementAndGet() + " messages total");
            result.setResponseCode("OK");
            return result;
        } catch (MqttException | IOException e) {
            result.sampleEnd(); // stop stopwatch
            result.setSuccessful(false);
            // get stack trace as a String to return as document data
            java.io.StringWriter stringWriter = new java.io.StringWriter();
            e.printStackTrace(new java.io.PrintWriter(stringWriter));
            result.setResponseData(stringWriter.toString(), null);
            result.setResponseMessage("Unable publish messages." + lineSeparator + "Exception: " + e.toString());
            result.setDataType(org.apache.jmeter.samplers.SampleResult.TEXT);
            result.setResponseCode("FAILED");
            return result;
        }
    }
}
