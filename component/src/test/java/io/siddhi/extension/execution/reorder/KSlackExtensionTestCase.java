/*
 * Copyright (c)  2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package io.siddhi.extension.execution.reorder;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.EventPrinter;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * This is the test case for KSlackExtension.
 * Created by miyurud on 8/10/15.
 */
public class KSlackExtensionTestCase {
    private static final Logger log = Logger.getLogger(KSlackExtensionTestCase.class);
    private volatile int count;
    private volatile boolean eventArrived;

    @BeforeMethod
    public void init() {
        count = 0;
        eventArrived = false;
    }

    @Test
    public void orderTest() throws InterruptedException {
        log.info("KSlackExtensionTestCase TestCase 1");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (eventtt long, price long, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from inputStream#reorder:kslack(eventtt, 1000L) " +
                "select eventtt, price, volume " +
                "insert into outputStream;";
        siddhiManager.setExtension("reorder:kslack", KSlackExtension.class);
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(
                inStreamDefinition + query);

        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {

            @Override
            public void receive(io.siddhi.core.event.Event[] events) {
                for (io.siddhi.core.event.Event event : events) {
                    count++;

                    if (count == 1) {
                        AssertJUnit.assertEquals(1L, event.getData()[0]);
                    }

                    if (count == 2) {
                        AssertJUnit.assertEquals(4L, event.getData()[0]);
                    }

                    if (count == 3) {
                        AssertJUnit.assertEquals(3L, event.getData()[0]);
                    }

                    if (count == 4) {
                        AssertJUnit.assertEquals(5L, event.getData()[0]);
                    }

                    if (count == 5) {
                        AssertJUnit.assertEquals(6L, event.getData()[0]);
                    }

                    if (count == 6) {
                        AssertJUnit.assertEquals(7L, event.getData()[0]);
                    }

                    if (count == 7) {
                        AssertJUnit.assertEquals(8L, event.getData()[0]);
                    }

                    if (count == 8) {
                        AssertJUnit.assertEquals(9L, event.getData()[0]);
                    }

                    if (count == 9) {
                        AssertJUnit.assertEquals(10L, event.getData()[0]);
                    }

                    if (count == 10) {
                        AssertJUnit.assertEquals(13L, event.getData()[0]);
                    }
                }
            }
        });


        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();

        //The following implements the out-of-order disorder handling scenario described in the
        //http://dl.acm.org/citation.cfm?doid=2675743.2771828
        inputHandler.send(new Object[]{1L, 700f, 100L});
        inputHandler.send(new Object[]{4L, 60.5f, 200L});
        inputHandler.send(new Object[]{3L, 60.5f, 200L});
        inputHandler.send(new Object[]{5L, 700f, 100L});
        inputHandler.send(new Object[]{6L, 60.5f, 200L});
        inputHandler.send(new Object[]{9L, 60.5f, 200L});
        inputHandler.send(new Object[]{7L, 700f, 100L});
        inputHandler.send(new Object[]{8L, 60.5f, 200L});
        inputHandler.send(new Object[]{10L, 60.5f, 200L});
        inputHandler.send(new Object[]{13L, 60.5f, 200L});

        Thread.sleep(2000);
        executionPlanRuntime.shutdown();
        AssertJUnit.assertTrue("Event count is at least 9:", count >= 9);
    }

    @Test(dependsOnMethods = "orderTest")
    public void orderTest2() throws InterruptedException {
        log.info("KSlackExtensionTestCase TestCase 2");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (eventtt long, price long, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from inputStream#reorder:kslack(eventtt, 1000L) " +
                "select eventtt, price, volume " +
                "insert into outputStream;";
        siddhiManager.setExtension("reorder:kslack", KSlackExtension.class);

        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(
                inStreamDefinition + query);

        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {

            @Override
            public void receive(io.siddhi.core.event.Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    count++;
                    if (count == 1) {
                        AssertJUnit.assertEquals(1f, event.getData()[1]);
                    }

                    if (count == 2) {
                        AssertJUnit.assertEquals(2f, event.getData()[1]);
                    }

                    if (count == 3) {
                        AssertJUnit.assertEquals(8f, event.getData()[1]);
                    }

                    if (count == 4) {
                        AssertJUnit.assertEquals(4f, event.getData()[1]);
                    }

                    if (count == 5) {
                        AssertJUnit.assertEquals(3f, event.getData()[1]);
                    }

                    if (count == 6) {
                        AssertJUnit.assertEquals(9f, event.getData()[1]);
                    }

                    if (count == 7) {
                        AssertJUnit.assertEquals(5f, event.getData()[1]);
                    }

                    if (count == 8) {
                        AssertJUnit.assertEquals(6f, event.getData()[1]);
                    }

                    if (count == 9) {
                        AssertJUnit.assertEquals(7f, event.getData()[1]);
                    }

                    if (count == 10) {
                        AssertJUnit.assertEquals(10f, event.getData()[1]);
                    }
                }
            }
        });


        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        long currentTime = System.currentTimeMillis();
        inputHandler.send(new Object[]{currentTime, 1f, 0});
        inputHandler.send(new Object[]{currentTime - 6000L, 2f, -6000L});
        inputHandler.send(new Object[]{currentTime - 500L, 3f, -500L});
        inputHandler.send(new Object[]{currentTime - 4000L, 4f, -4000L}); //+3
        inputHandler.send(new Object[]{currentTime, 5f, 0});
        inputHandler.send(new Object[]{currentTime, 6f, 0});
        inputHandler.send(new Object[]{currentTime, 7f, 0});
        inputHandler.send(new Object[]{currentTime - 6000L, 8f, -6000L});
        inputHandler.send(new Object[]{currentTime - 500L, 9f, -500L});
        inputHandler.send(new Object[]{currentTime, 10f, 0});
        inputHandler.send(new Object[]{currentTime + 1000L, 11f, +1000L});

        Thread.sleep(3500);
        executionPlanRuntime.shutdown();
        AssertJUnit.assertEquals("Event count", 11, count);
    }

    @Test(dependsOnMethods = "orderTest2")
    public void orderTest23() throws InterruptedException {
        log.info("KSlackExtensionTestCase TestCase 23");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (eventtt long, price long, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from inputStream#reorder:kslack(eventtt, 1000L) " +
                "select eventtt, price, volume " +
                "insert into outputStream;";
        siddhiManager.setExtension("reorder:kslack", KSlackExtension.class);

        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(
                inStreamDefinition + query);

        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {

            @Override
            public void receive(io.siddhi.core.event.Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    count++;
                    if (count == 1) {
                        AssertJUnit.assertEquals(1f, event.getData()[1]);
                    }

                    if (count == 2) {
                        AssertJUnit.assertEquals(2f, event.getData()[1]);
                    }

                    if (count == 3) {
                        AssertJUnit.assertEquals(4f, event.getData()[1]);
                    }

                    if (count == 4) {
                        AssertJUnit.assertEquals(3f, event.getData()[1]);
                    }

                    if (count == 5) {
                        AssertJUnit.assertEquals(5f, event.getData()[1]);
                    }

                    if (count == 6) {
                        AssertJUnit.assertEquals(8f, event.getData()[1]);
                    }

                    if (count == 7) {
                        AssertJUnit.assertEquals(6f, event.getData()[1]);
                    }

                    if (count == 8) {
                        AssertJUnit.assertEquals(9f, event.getData()[1]);
                    }

                    if (count == 9) {
                        AssertJUnit.assertEquals(7f, event.getData()[1]);
                    }

                    if (count == 10) {
                        AssertJUnit.assertEquals(10f, event.getData()[1]);
                    }
                }
            }
        });


        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();

        long currentTime = System.currentTimeMillis();
        inputHandler.send(new Object[]{currentTime, 1f, 0});
        inputHandler.send(new Object[]{currentTime - 6000L, 2f, -6000L});
        inputHandler.send(new Object[]{currentTime - 500L, 3f, -500L});
        Thread.sleep(2000);
        currentTime = System.currentTimeMillis();
        inputHandler.send(new Object[]{currentTime - 4000L, 4f, -4000L}); //+3
        inputHandler.send(new Object[]{currentTime, 5f, 0});
        inputHandler.send(new Object[]{currentTime, 6f, 0});
        Thread.sleep(4000);
        currentTime = System.currentTimeMillis();
        inputHandler.send(new Object[]{currentTime, 7f, 0});
        inputHandler.send(new Object[]{currentTime - 6000L, 8f, -6000L});
        inputHandler.send(new Object[]{currentTime - 500L, 9f, -500L});
        inputHandler.send(new Object[]{currentTime, 10f, 0});
        inputHandler.send(new Object[]{currentTime + 1000L, 11f, +1000L});
        Thread.sleep(3500);
        executionPlanRuntime.shutdown();
        AssertJUnit.assertEquals("Event count", 11, count);
    }

    @Test(dependsOnMethods = "orderTest23")
    public void orderTest24() throws InterruptedException {
        log.info("KSlackExtensionTestCase TestCase 24");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (eventtt long, price long, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from inputStream#reorder:kslack(eventtt, 1000L) " +
                "select eventtt, price, volume " +
                "insert into outputStream;";
        siddhiManager.setExtension("reorder:kslack", KSlackExtension.class);

        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(
                inStreamDefinition + query);

        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {

            @Override
            public void receive(io.siddhi.core.event.Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    count++;
                    if (count == 1) {
                        AssertJUnit.assertEquals(1f, event.getData()[1]);
                    }

                    if (count == 2) {
                        AssertJUnit.assertEquals(2f, event.getData()[1]);
                    }

                    if (count == 3) {
                        AssertJUnit.assertEquals(3f, event.getData()[1]);
                    }

                    if (count == 4) {
                        AssertJUnit.assertEquals(4f, event.getData()[1]);
                    }

                    if (count == 5) {
                        AssertJUnit.assertEquals(5f, event.getData()[1]);
                    }

                    if (count == 6) {
                        AssertJUnit.assertEquals(8f, event.getData()[1]);
                    }

                    if (count == 7) {
                        AssertJUnit.assertEquals(6f, event.getData()[1]);
                    }

                    if (count == 8) {
                        AssertJUnit.assertEquals(9f, event.getData()[1]);
                    }

                    if (count == 9) {
                        AssertJUnit.assertEquals(7f, event.getData()[1]);
                    }

                    if (count == 10) {
                        AssertJUnit.assertEquals(10f, event.getData()[1]);
                    }
                }
            }
        });


        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();

        long currentTime = System.currentTimeMillis();
        inputHandler.send(new Object[]{currentTime, 1f, 0});
        inputHandler.send(new Object[]{currentTime - 6000L, 2f, -6000L});
        inputHandler.send(new Object[]{currentTime - 500L, 3f, -500L});
        Thread.sleep(2000);
        currentTime = System.currentTimeMillis();
        inputHandler.send(new Object[]{currentTime - 500L, 4f, -500L}); //+3
        inputHandler.send(new Object[]{currentTime, 5f, 0});
        inputHandler.send(new Object[]{currentTime, 6f, 0});
        Thread.sleep(4000);
        currentTime = System.currentTimeMillis();
        inputHandler.send(new Object[]{currentTime, 7f, 0});
        inputHandler.send(new Object[]{currentTime - 6000L, 8f, -6000L});
        inputHandler.send(new Object[]{currentTime - 500L, 9f, -500L});
        inputHandler.send(new Object[]{currentTime, 10f, 0});
        inputHandler.send(new Object[]{currentTime + 1000L, 11f, +1000L});
        Thread.sleep(3500);
        executionPlanRuntime.shutdown();
        AssertJUnit.assertEquals("Event count", 11, count);
    }

    @Test//(dependsOnMethods = "orderTest24")
    public void orderTest25() throws InterruptedException {
        log.info("KSlackExtensionTestCase TestCase 25");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (eventtt long, price long, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from inputStream#reorder:kslack(eventtt) " +
                "select eventtt, price, volume " +
                "insert into outputStream;";
        siddhiManager.setExtension("reorder:kslack", KSlackExtension.class);

        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(
                inStreamDefinition + query);

        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {

            @Override
            public void receive(io.siddhi.core.event.Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    count++;
                    if (count == 1) {
                        AssertJUnit.assertEquals(1f, event.getData()[1]);
                    }

                    if (count == 2) {
                        AssertJUnit.assertEquals(2f, event.getData()[1]);
                    }

                    if (count == 3) {
                        AssertJUnit.assertEquals(8f, event.getData()[1]);
                    }
                }
            }
        });


        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        long currentTime = System.currentTimeMillis();
        inputHandler.send(new Event[]{
                new Event(System.currentTimeMillis(), new Object[]{currentTime, 1f, 0}),
                new Event(System.currentTimeMillis(), new Object[]{currentTime - 6000L, 2f, -6000L}),
                new Event(System.currentTimeMillis(), new Object[]{currentTime - 500L, 3f, -500L}),
                new Event(System.currentTimeMillis(), new Object[]{currentTime - 4000L, 4f, -4000L}),
                new Event(System.currentTimeMillis(), new Object[]{currentTime, 5f, 0}),
                new Event(System.currentTimeMillis(), new Object[]{currentTime, 6f, 0}),
                new Event(System.currentTimeMillis(), new Object[]{currentTime, 7f, 0}),
                new Event(System.currentTimeMillis(), new Object[]{currentTime - 6000L, 8f, -6000L}),
                new Event(System.currentTimeMillis(), new Object[]{currentTime - 500L, 9f, -500L}),
                new Event(System.currentTimeMillis(), new Object[]{currentTime, 10f, 0}),
                new Event(System.currentTimeMillis(), new Object[]{currentTime + 1000L, 11f, +1000L}),

        });
        Thread.sleep(3500);
        executionPlanRuntime.shutdown();
        AssertJUnit.assertEquals("Event count", 3, count);
    }

    @Test(dependsOnMethods = "orderTest25")
    public void orderTest26() throws InterruptedException {
        log.info("KSlackExtensionTestCase TestCase 26");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (eventtt long, price long, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from inputStream#reorder:kslack(eventtt, 1000L, true) " +
                "select eventtt, price, volume " +
                "insert into outputStream;";
        siddhiManager.setExtension("reorder:kslack", KSlackExtension.class);

        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(
                inStreamDefinition + query);

        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {

            @Override
            public void receive(io.siddhi.core.event.Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    count++;
                    if (count == 1) {
                        AssertJUnit.assertEquals(1f, event.getData()[1]);
                    }

                    if (count == 2) {
                        AssertJUnit.assertEquals(4f, event.getData()[1]);
                    }

                    if (count == 3) {
                        AssertJUnit.assertEquals(5f, event.getData()[1]);
                    }

                    if (count == 4) {
                        AssertJUnit.assertEquals(6f, event.getData()[1]);
                    }

                    if (count == 5) {
                        AssertJUnit.assertEquals(9f, event.getData()[1]);
                    }

                    if (count == 6) {
                        AssertJUnit.assertEquals(7f, event.getData()[1]);
                    }

                    if (count == 7) {
                        AssertJUnit.assertEquals(10f, event.getData()[1]);
                    }

                    if (count == 8) {
                        AssertJUnit.assertEquals(11f, event.getData()[1]);
                    }
                }
            }
        });


        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();

        long currentTime = System.currentTimeMillis();
        inputHandler.send(new Object[]{currentTime, 1f, 0});
        inputHandler.send(new Object[]{currentTime - 6000L, 2f, -6000L});
        inputHandler.send(new Object[]{currentTime - 500L, 3f, -500L});
        Thread.sleep(2000);
        currentTime = System.currentTimeMillis();
        inputHandler.send(new Object[]{currentTime - 500L, 4f, -500L}); //+3
        inputHandler.send(new Object[]{currentTime, 5f, 0});
        inputHandler.send(new Object[]{currentTime, 6f, 0});
        Thread.sleep(4000);
        currentTime = System.currentTimeMillis();
        inputHandler.send(new Object[]{currentTime, 7f, 0});
        inputHandler.send(new Object[]{currentTime - 6000L, 8f, -6000L});
        inputHandler.send(new Object[]{currentTime - 500L, 9f, -500L});
        inputHandler.send(new Object[]{currentTime, 10f, 0});
        inputHandler.send(new Object[]{currentTime + 1000L, 11f, +1000L});
        Thread.sleep(3500);
        executionPlanRuntime.shutdown();
        AssertJUnit.assertEquals("Event count", 8, count);
    }

    @Test(dependsOnMethods = "orderTest26")
    public void orderTest27() throws InterruptedException {
        log.info("KSlackExtensionTestCase TestCase 27");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (eventtt long, price long, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from inputStream#reorder:kslack(eventtt, true) " +
                "select eventtt, price, volume " +
                "insert into outputStream;";
        siddhiManager.setExtension("reorder:kslack", KSlackExtension.class);

        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(
                inStreamDefinition + query);

        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {

            @Override
            public void receive(io.siddhi.core.event.Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    count++;
                    if (count == 1) {
                        AssertJUnit.assertEquals(1f, event.getData()[1]);
                    }

                    if (count == 2) {
                        AssertJUnit.assertEquals(4f, event.getData()[1]);
                    }

                    if (count == 3) {
                        AssertJUnit.assertEquals(5f, event.getData()[1]);
                    }

                    if (count == 4) {
                        AssertJUnit.assertEquals(6f, event.getData()[1]);
                    }
                }
            }
        });


        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();

        long currentTime = System.currentTimeMillis();
        inputHandler.send(new Object[]{currentTime, 1f, 0});
        inputHandler.send(new Object[]{currentTime - 6000L, 2f, -6000L});
        inputHandler.send(new Object[]{currentTime - 500L, 3f, -500L});
        Thread.sleep(2000);
        currentTime = System.currentTimeMillis();
        inputHandler.send(new Object[]{currentTime - 500L, 4f, -500L}); //+3
        inputHandler.send(new Object[]{currentTime, 5f, 0});
        inputHandler.send(new Object[]{currentTime, 6f, 0});
        Thread.sleep(4000);
        currentTime = System.currentTimeMillis();
        inputHandler.send(new Object[]{currentTime, 7f, 0});
        inputHandler.send(new Object[]{currentTime - 6000L, 8f, -6000L});
        inputHandler.send(new Object[]{currentTime - 500L, 9f, -500L});
        inputHandler.send(new Object[]{currentTime, 10f, 0});
        inputHandler.send(new Object[]{currentTime + 1000L, 11f, +1000L});
        Thread.sleep(3500);
        executionPlanRuntime.shutdown();
        AssertJUnit.assertEquals("Event count", 4, count);
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void orderTest3() throws InterruptedException {
        log.info("KSlackExtensionTestCase for invalid length ");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition =
                "define stream inputStream (eventtt long, price long, volume long,data string,data2" + " string);";
        String query = ("@info(name = 'query1') from inputStream#reorder:kslack(eventtt,price,volume,data,data2) "
                + "select eventtt, price, " + "volume " + "insert into outputStream;");
        siddhiManager.setExtension("reorder:kslack", KSlackExtension.class);
        siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);

    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void orderTest4() throws InterruptedException {
        log.info("KSlackExtensionTestCase Invalid type first argument length 1");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (eventtt string, price long, volume long);";
        String query = ("@info(name = 'query1') from inputStream#reorder:kslack(eventtt) select eventtt, price, "
                + "volume " + "insert into outputStream;");
        siddhiManager.setExtension("reorder:kslack", KSlackExtension.class);
        siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);

    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void orderTest5() throws InterruptedException {
        log.info("KSlackExtensionTestCase Invalid type first argument length 2");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (eventtt string, price long, volume long);";
        String query = ("@info(name = 'query1') from inputStream#reorder:kslack(eventtt, 1000L) select eventtt, price, "
                + "volume " + "insert into outputStream;");
        siddhiManager.setExtension("reorder:kslack", KSlackExtension.class);
        siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);

    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void orderTest6() throws InterruptedException {
        log.info("KSlackExtensionTestCase Invalid type second argument length 2");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (eventtt long, price string, volume long);";
        String query = ("@info(name = 'query1') from inputStream#reorder:kslack(eventtt,price) select eventtt, "
                + "price, " + "volume " + "insert into outputStream;");
        siddhiManager.setExtension("reorder:kslack", KSlackExtension.class);
        siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);

    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void orderTest7() throws InterruptedException {
        log.info("KSlackExtensionTestCase for invalid type first argument length 3");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (eventtt string, price long, volume long);";
        String query = ("@info(name = 'query1') from inputStream#reorder:kslack(eventtt,price, 1000L) "
                + "select eventtt, price, " + "volume " + "insert into outputStream;");
        siddhiManager.setExtension("reorder:kslack", KSlackExtension.class);
        siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);

    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void orderTest8() throws InterruptedException {
        log.info("KSlackExtensionTestCase Invalid type second argument length 3");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (eventtt long, price string, volume long);";
        String query = ("@info(name = 'query1') from inputStream#reorder:kslack(eventtt,price, 1000L) select eventtt, "
                + "price, " + "volume " + "insert into outputStream;");
        siddhiManager.setExtension("reorder:kslack", KSlackExtension.class);
        siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);

    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void orderTest9() throws InterruptedException {
        log.info("KSlackExtensionTestCase Invalid type third argument length 3");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (eventtt long, price string, volume long);";
        String query = ("@info(name = 'query1') from inputStream#reorder:kslack(eventtt,12l,'maxk') select eventtt, "
                + "price, " + "volume " + "insert into outputStream;");
        siddhiManager.setExtension("reorder:kslack", KSlackExtension.class);
        siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);

    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void orderTest10() throws InterruptedException {
        log.info("KSlackExtensionTestCase for invalid type first argument length four");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (eventtt string, price long, volume long);";
        String query = ("@info(name = 'query1') from inputStream#reorder:kslack(eventtt,price,1000L, volume) "
                + "select eventtt, price, " + "volume " + "insert into outputStream;");
        siddhiManager.setExtension("reorder:kslack", KSlackExtension.class);
        siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);

    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void orderTest11() throws InterruptedException {
        log.info("KSlackExtensionTestCase for invalid type second argument length four");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (eventtt long, price string, volume long);";
        String query = ("@info(name = 'query1') from inputStream#reorder:kslack(eventtt,price,1000L, volume) "
                + "select eventtt, price, " + "volume " + "insert into outputStream;");
        siddhiManager.setExtension("reorder:kslack", KSlackExtension.class);
        siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);

    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void orderTest12() throws InterruptedException {
        log.info("KSlackExtensionTestCase for invalid type third argument length four");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (eventtt long, price long, volume string,data long);";
        String query = ("@info(name = 'query1') from inputStream#reorder:kslack(eventtt,12l,'maxK',true) "
                + "select eventtt, price, " + "volume " + "insert into outputStream;");
        siddhiManager.setExtension("reorder:kslack", KSlackExtension.class);
        siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);

    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void orderTest13() throws InterruptedException {
        log.info("KSlackExtensionTestCase for invalid type fourth argument length four");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (eventtt long, price long, volume string,data long);";
        String query = ("@info(name = 'query1') from inputStream#reorder:kslack(eventtt,12l,15l,'expireFlag') "
                + "select eventtt, price, " + "volume " + "insert into outputStream;");
        siddhiManager.setExtension("reorder:kslack", KSlackExtension.class);
        siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);

    }
}
