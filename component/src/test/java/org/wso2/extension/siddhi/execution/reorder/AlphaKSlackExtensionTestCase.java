/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.extension.siddhi.execution.reorder;

import junit.framework.Assert;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;

public class AlphaKSlackExtensionTestCase {
    static final Logger log = Logger.getLogger(AlphaKSlackExtensionTestCase.class);
    private volatile int count;

    @Test
    public void Testcase() throws InterruptedException {
        log.info("Alpha K-Slack Extension Testcase");

        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (eventtt long,data double);";
        String query = ("@info(name = 'query1') from inputStream#reorder:akslack(eventtt, " +
                "data, 20l) select  " +
                "eventtt, data " +
                "insert into outputStream;");

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime
                (inStreamDefinition + query);

        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {

            @Override
            public void receive(org.wso2.siddhi.core.event.Event[] events) {

                for (org.wso2.siddhi.core.event.Event event : events) {
                    count++;

                    if (count == 1) {
                        Assert.assertEquals(1l, event.getData()[0]);
                    }

                    if (count == 2) {
                        Assert.assertEquals(4l, event.getData()[0]);
                    }

                    if (count == 3) {
                        Assert.assertEquals(3l, event.getData()[0]);
                    }

                    if (count == 4) {
                        Assert.assertEquals(5l, event.getData()[0]);
                    }

                    if (count == 5) {
                        Assert.assertEquals(6l, event.getData()[0]);
                    }

                    if (count == 6) {
                        Assert.assertEquals(7l, event.getData()[0]);
                    }

                    if (count == 7) {
                        Assert.assertEquals(8l, event.getData()[0]);
                    }

                    if (count == 8) {
                        Assert.assertEquals(9l, event.getData()[0]);
                    }

                    if (count == 9) {
                        Assert.assertEquals(10l, event.getData()[0]);
                    }

                    if (count == 10) {
                        Assert.assertEquals(11l, event.getData()[0]);
                    }

                    if (count == 11) {
                        Assert.assertEquals(12l, event.getData()[0]);
                    }

                    if (count == 12) {
                        Assert.assertEquals(13l, event.getData()[0]);
                    }

                    if (count == 13) {
                        Assert.assertEquals(14l, event.getData()[0]);
                    }

                    if (count == 14) {
                        Assert.assertEquals(15l, event.getData()[0]);
                    }

                    if (count == 15) {
                        Assert.assertEquals(16l, event.getData()[0]);
                    }

                    if (count == 16) {
                        Assert.assertEquals(17l, event.getData()[0]);
                    }

                    if (count == 17) {
                        Assert.assertEquals(18l, event.getData()[0]);
                    }

                    if (count == 18) {
                        Assert.assertEquals(19l, event.getData()[0]);
                    }

                    if (count == 19) {
                        Assert.assertEquals(20l, event.getData()[0]);
                    }

                    if (count == 20) {
                        Assert.assertEquals(22l, event.getData()[0]);
                    }
                    if (count == 21) {
                        Assert.assertEquals(21l, event.getData()[0]);
                    }
                    if (count == 22) {
                        Assert.assertEquals(23l, event.getData()[0]);
                    }
                    if (count == 23) {
                        Assert.assertEquals(24l, event.getData()[0]);
                    }
                    if (count == 24) {
                        Assert.assertEquals(25l, event.getData()[0]);
                    }
                    if (count == 25) {
                        Assert.assertEquals(26l, event.getData()[0]);
                    }
                    if (count == 26) {
                        Assert.assertEquals(27l, event.getData()[0]);
                    }
                    if (count == 27) {
                        Assert.assertEquals(29l, event.getData()[0]);
                    }
                    if (count == 28) {
                        Assert.assertEquals(30l, event.getData()[0]);
                    }
                    if (count == 29) {
                        Assert.assertEquals(31l, event.getData()[0]);
                    }
                    if (count == 30) {
                        Assert.assertEquals(28l, event.getData()[0]);
                    }
                    if (count == 31) {
                        Assert.assertEquals(32l, event.getData()[0]);
                    }
                    if (count == 32) {
                        Assert.assertEquals(33l, event.getData()[0]);
                    }
                    if (count == 33) {
                        Assert.assertEquals(34l, event.getData()[0]);
                    }

                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();

        inputHandler.send(new Object[]{1l, 79.0});
        inputHandler.send(new Object[]{4l, 60.0});
        inputHandler.send(new Object[]{3l, 65.0});
        inputHandler.send(new Object[]{5l, 30.0});
        inputHandler.send(new Object[]{6l, 43.0});
        inputHandler.send(new Object[]{9l, 90.0});
        inputHandler.send(new Object[]{7l, 98.7});
        inputHandler.send(new Object[]{8l, 80.0});
        inputHandler.send(new Object[]{10l, 100.0});
        inputHandler.send(new Object[]{12l, 19.0});
        inputHandler.send(new Object[]{13l, 45.0});
        inputHandler.send(new Object[]{14l, 110.0});
        inputHandler.send(new Object[]{11l, 92.0});
        inputHandler.send(new Object[]{15l, 29.0});
        inputHandler.send(new Object[]{17l, 55.0});
        inputHandler.send(new Object[]{18l, 61.0});
        inputHandler.send(new Object[]{19l, 33.0});
        inputHandler.send(new Object[]{16l, 30.0});
        inputHandler.send(new Object[]{20l, 66.0});
        inputHandler.send(new Object[]{22l, 42.0});
        inputHandler.send(new Object[]{23l, 61.0});
        inputHandler.send(new Object[]{24l, 33.0});
        inputHandler.send(new Object[]{25l, 30.0});
        inputHandler.send(new Object[]{26l, 14.0});
        inputHandler.send(new Object[]{21l, 42.0});
        inputHandler.send(new Object[]{27l, 45.0});
        inputHandler.send(new Object[]{29l, 29.0});
        inputHandler.send(new Object[]{30l, 9.0});
        inputHandler.send(new Object[]{31l, 13.0});
        inputHandler.send(new Object[]{33l, 66.0});
        inputHandler.send(new Object[]{28l, 29.0});
        inputHandler.send(new Object[]{34l, 51.0});
        inputHandler.send(new Object[]{32l, 27.0});



        Thread.sleep(2000);
        executionPlanRuntime.shutdown();

    }
}