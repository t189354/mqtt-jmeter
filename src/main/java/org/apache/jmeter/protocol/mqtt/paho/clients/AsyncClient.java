/*
 * Copyright 2016 Hemika Yasinda Kodikara
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

package org.apache.jmeter.protocol.mqtt.paho.clients;

import org.apache.jmeter.protocol.mqtt.data.objects.Message;
import org.apache.jorphan.logging.LoggingManager;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;

import java.io.File;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;


public class AsyncClient extends BaseClient {

    private static final org.apache.log.Logger log = LoggingManager.getLoggerForClass();
    private MqttAsyncClient client;
    private String brokerUrl;

    /**
     * Constructs an instance of the sample client wrapper
     *
     * @param brokerUrl    the url to connect to
     * @param clientId     the client id to connect with
     * @param cleanSession clear state at end of connection or not (durable or non-durable subscriptions)
     * @param userName     the username to connect with
     * @param password     the password for the user
     * @throws MqttException the exception
     */
    public AsyncClient(String brokerUrl, String clientId, boolean cleanSession,
                       String userName, String password, int keepAlive) throws MqttException {
        this.brokerUrl = brokerUrl;

        String testPlanFileDir = System.getProperty("java.io.tmpdir") + File.separator + "mqtt" + File.separator +
                                                            clientId + File.separator + Thread.currentThread().getId();
        MqttDefaultFilePersistence dataStore = new MqttDefaultFilePersistence(testPlanFileDir);

        try {
            // Construct the connection options object that contains connection parameters
            // such as cleanSession and LWT
            MqttConnectOptions conOpt = new MqttConnectOptions();
            conOpt.setCleanSession(cleanSession);
            if (password != null && !password.isEmpty()) {
                conOpt.setPassword(password.toCharArray());
            }
            if (userName != null && !userName.isEmpty()) {
                conOpt.setUserName(userName);
            }

            // Setting keep alive time
            conOpt.setKeepAliveInterval(keepAlive);

            // Construct a non-blocking MQTT client instance
            client = new MqttAsyncClient(this.brokerUrl, clientId, dataStore);

            // Set this wrapper as the callback handler
            client.setCallback(this);

            // Connect to the MQTT server
            // issue a non-blocking connect and then use the token to wait until the
            // connect completes. An exception is thrown if connect fails.
            log.info("Connecting to " + brokerUrl + " with client ID '" + client.getClientId() + "' and cleanSession is " +
                    cleanSession + " as an async clientt");
            IMqttToken conToken = client.connect(conOpt, null, null);
            conToken.waitForCompletion();
            log.info(client.getClientId() + " Connected");

        } catch (MqttException e) {
            log.warn("Unable to set up client " + clientId + ": " + e.toString());
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     * @return
     */
    @Override
    public long publish(String topicName, int qos, byte[] payload, boolean isRetained, long timeout) throws MqttException {
        // Construct the message to send
        MqttMessage message = new MqttMessage(payload);
        message.setRetained(isRetained);
        message.setQos(qos);

        // Send the message to the server, control is returned as soon
        // as the MQTT client has accepted to deliver the message.
        // Use the delivery token to wait until the message has been
        // delivered
        long start = System.nanoTime();
        IMqttDeliveryToken pubToken = client.publish(topicName, message, null, null);
        if (timeout > 0) {
            pubToken.waitForCompletion(timeout);
        } else {
            pubToken.waitForCompletion();
        }
        long duration = System.nanoTime() - start;
        if (log.isDebugEnabled()) log.debug(client.getClientId() + " published to " + topicName);
        return duration;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void subscribe(String topicName, int qos, long timeout) throws MqttException {
        mqttMessageStorage = new ConcurrentLinkedQueue<>();
        receivedMessageCounter = new AtomicLong(0);

        // Subscribe to the requested topic.
        // Control is returned as soon client has accepted to deliver the subscription.
        // Use a token to wait until the subscription is in place.
        log.info(client.getClientId() + " subscribing to topic \"" + topicName + "\" qos " + qos);
        IMqttToken subToken = client.subscribe(topicName, qos, null, null);
        if (timeout > 0) {
            subToken.waitForCompletion(timeout);
        } else {
            subToken.waitForCompletion();
        }
        log.info(client.getClientId() + " subscribed to topic \"" + topicName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void connectionLost(Throwable cause) {
        // Called when the connection to the server has been lost.
        // An application may choose to implement reconnection
        // logic at this point. This sample simply exits.
        log.warn(client.getClientId() + " connection to " + brokerUrl + " lost!" + cause);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        // Called when a message has been delivered to the
        // server. The token passed in here is the same one
        // that was passed to or returned from the original call to publish.
        // This allows applications to perform asynchronous
        // delivery without blocking until delivery completes.
        //
        // This sample demonstrates asynchronous deliver and
        // uses the token.waitForCompletion() call in the main thread which
        // blocks until the delivery has completed.
        // Additionally the deliveryComplete method will be called if
        // the callback is set on the client
        //
        // If the connection to the server breaks before delivery has completed
        // delivery of a message will complete after the client has re-connected.
        // The getPendinTokens method will provide tokens for any messages
        // that are still to be delivered.
        try {
            if (log.isDebugEnabled()) log.debug("Delivery complete callback: Publish Completed " + token.getMessage());
        } catch (Exception ex) {
            log.warn(client.getClientId() + " exception in delivery complete callback" + ex);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void messageArrived(String topic, MqttMessage mqttMessage) {
        Message newMessage = new Message(mqttMessage);
        mqttMessageStorage.add(newMessage);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void disconnect() throws MqttException {
        // Disconnect the client
        // Issue the disconnect and then use the token to wait until
        // the disconnect completes.
        log.info(client.getClientId() + " disconnecting");
        IMqttToken discToken = client.disconnect(null, null);
        discToken.waitForCompletion();
        log.info(client.getClientId() + " disconnected");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isConnected() {
        return client.isConnected();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        try {
            client.disconnect();
        } catch (MqttException e) {
            log.error(client.getClientId() + " error disconnecting" + e.getMessage(), e);
        }
    }
}