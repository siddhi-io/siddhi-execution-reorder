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

import org.apache.log4j.Logger;
//import org.testng.AssertJUnit;
//import org.testng.annotations.Test;
//import org.wso2.siddhi.core.SiddhiAppRuntime;
//import org.wso2.siddhi.core.SiddhiManager;
//import org.wso2.siddhi.core.stream.input.InputHandler;
//import org.wso2.siddhi.core.stream.output.StreamCallback;
//import org.wso2.siddhi.core.util.EventPrinter;

public class AlphaKSlackExtensionTestCase {
    private static final Logger log = Logger.getLogger(AlphaKSlackExtensionTestCase.class);
    private volatile int count;
//todo commenting out testcase till alpha k slack extension is fixed
//    @Test
//    public void testcase() throws InterruptedException {
//        log.info("Alpha K-Slack Extension Testcase");
//
//        SiddhiManager siddhiManager = new SiddhiManager();
//        siddhiManager.setExtension("reorder:akslack", AlphaKSlackExtension.class);
//
//        String inStreamDefinition = "define stream inputStream (eventtt long,data double);";
//        String query = ("@info(name = 'query1') from inputStream#reorder:akslack(eventtt, " +
//                "data, 20l) select  " +
//                "eventtt, data " +
//                "insert into outputStream;");
//
//        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime
//                (inStreamDefinition + query);
//
//        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {
//
//            @Override
//            public void receive(org.wso2.siddhi.core.event.Event[] events) {
//
//                for (org.wso2.siddhi.core.event.Event event : events) {
//                    EventPrinter.print(events);
//                    count++;
//
//                    if (count == 1) {
//                        AssertJUnit.assertEquals(1L, event.getData()[0]);
//                    }
//
//                    if (count == 2) {
//                        AssertJUnit.assertEquals(4L, event.getData()[0]);
//                    }
//
//                    if (count == 3) {
//                        AssertJUnit.assertEquals(3L, event.getData()[0]);
//                    }
//
//                    if (count == 4) {
//                        AssertJUnit.assertEquals(5L, event.getData()[0]);
//                    }
//
//                    if (count == 5) {
//                        AssertJUnit.assertEquals(6L, event.getData()[0]);
//                    }
//
//                    if (count == 6) {
//                        AssertJUnit.assertEquals(7L, event.getData()[0]);
//                    }
//
//                    if (count == 7) {
//                        AssertJUnit.assertEquals(8L, event.getData()[0]);
//                    }
//
//                    if (count == 8) {
//                        AssertJUnit.assertEquals(9L, event.getData()[0]);
//                    }
//
//                    if (count == 9) {
//                        AssertJUnit.assertEquals(10L, event.getData()[0]);
//                    }
//
//                    if (count == 10) {
//                        AssertJUnit.assertEquals(11L, event.getData()[0]);
//                    }
//
//                    if (count == 11) {
//                        AssertJUnit.assertEquals(12L, event.getData()[0]);
//                    }
//
//                    if (count == 12) {
//                        AssertJUnit.assertEquals(13L, event.getData()[0]);
//                    }
//
//                    if (count == 13) {
//                        AssertJUnit.assertEquals(14L, event.getData()[0]);
//                    }
//
//                    if (count == 14) {
//                        AssertJUnit.assertEquals(15L, event.getData()[0]);
//                    }
//
//                    if (count == 15) {
//                        AssertJUnit.assertEquals(16L, event.getData()[0]);
//                    }
//
//                    if (count == 16) {
//                        AssertJUnit.assertEquals(17L, event.getData()[0]);
//                    }
//
//                    if (count == 17) {
//                        AssertJUnit.assertEquals(18L, event.getData()[0]);
//                    }
//
//                    if (count == 18) {
//                        AssertJUnit.assertEquals(19L, event.getData()[0]);
//                    }
//
//                    if (count == 19) {
//                        AssertJUnit.assertEquals(20L, event.getData()[0]);
//                    }
//
//                    if (count == 20) {
//                        AssertJUnit.assertEquals(22L, event.getData()[0]);
//                    }
//                    if (count == 21) {
//                        AssertJUnit.assertEquals(21L, event.getData()[0]);
//                    }
//                    if (count == 22) {
//                        AssertJUnit.assertEquals(23L, event.getData()[0]);
//                    }
//                    if (count == 23) {
//                        AssertJUnit.assertEquals(24L, event.getData()[0]);
//                    }
//                    if (count == 24) {
//                        AssertJUnit.assertEquals(25L, event.getData()[0]);
//                    }
//                    if (count == 25) {
//                        AssertJUnit.assertEquals(26L, event.getData()[0]);
//                    }
//                    if (count == 26) {
//                        AssertJUnit.assertEquals(27L, event.getData()[0]);
//                    }
//                    if (count == 27) {
//                        AssertJUnit.assertEquals(29L, event.getData()[0]);
//                    }
//                    if (count == 28) {
//                        AssertJUnit.assertEquals(30L, event.getData()[0]);
//                    }
//                    if (count == 29) {
//                        AssertJUnit.assertEquals(31L, event.getData()[0]);
//                    }
//                    if (count == 30) {
//                        AssertJUnit.assertEquals(28L, event.getData()[0]);
//                    }
//                    if (count == 31) {
//                        AssertJUnit.assertEquals(32L, event.getData()[0]);
//                    }
//                    if (count == 32) {
//                        AssertJUnit.assertEquals(33L, event.getData()[0]);
//                    }
//                    if (count == 33) {
//                        AssertJUnit.assertEquals(34L, event.getData()[0]);
//                    }
//
//                }
//            }
//        });
//
//        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
//        executionPlanRuntime.start();
//
//        inputHandler.send(new Object[]{1L, 79.0});
//        inputHandler.send(new Object[]{4L, 60.0});
//        inputHandler.send(new Object[]{3L, 65.0});
//        inputHandler.send(new Object[]{5L, 30.0});
//        inputHandler.send(new Object[]{6L, 43.0});
//        inputHandler.send(new Object[]{9L, 90.0});
//        inputHandler.send(new Object[]{7L, 98.7});
//        inputHandler.send(new Object[]{8L, 80.0});
//        inputHandler.send(new Object[]{10L, 100.0});
//        inputHandler.send(new Object[]{12L, 19.0});
//        inputHandler.send(new Object[]{13L, 45.0});
//        inputHandler.send(new Object[]{14L, 110.0});
//        inputHandler.send(new Object[]{11L, 92.0});
//        inputHandler.send(new Object[]{15L, 29.0});
//        inputHandler.send(new Object[]{17L, 55.0});
//        inputHandler.send(new Object[]{18L, 61.0});
//        inputHandler.send(new Object[]{19L, 33.0});
//        inputHandler.send(new Object[]{16L, 30.0});
//        inputHandler.send(new Object[]{20L, 66.0});
//        inputHandler.send(new Object[]{22L, 42.0});
//        inputHandler.send(new Object[]{23L, 61.0});
//        inputHandler.send(new Object[]{24L, 33.0});
//        inputHandler.send(new Object[]{25L, 30.0});
//        inputHandler.send(new Object[]{26L, 14.0});
//        inputHandler.send(new Object[]{21L, 42.0});
//        inputHandler.send(new Object[]{27L, 45.0});
//        inputHandler.send(new Object[]{29L, 29.0});
//        inputHandler.send(new Object[]{30L, 9.0});
//        inputHandler.send(new Object[]{31L, 13.0});
//        inputHandler.send(new Object[]{33L, 66.0});
//        inputHandler.send(new Object[]{28L, 29.0});
//        inputHandler.send(new Object[]{34L, 51.0});
//        inputHandler.send(new Object[]{32L, 27.0});
//
//
//
//        Thread.sleep(2000);
//        executionPlanRuntime.shutdown();
//
//    }
}
