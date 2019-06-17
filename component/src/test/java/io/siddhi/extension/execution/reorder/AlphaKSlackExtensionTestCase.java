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

package io.siddhi.extension.execution.reorder;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.exception.CannotRestoreSiddhiAppStateException;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.SiddhiTestHelper;
import io.siddhi.core.util.persistence.InMemoryPersistenceStore;
import io.siddhi.core.util.persistence.PersistenceStore;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class AlphaKSlackExtensionTestCase {
    private static final Logger log = Logger.getLogger(AlphaKSlackExtensionTestCase.class);
    private volatile AtomicInteger count = new AtomicInteger(0);
    private long waitTime = 300;
    private long timeout = 2000;


    @Test
    public void testcase() throws InterruptedException {
        log.info("Alpha K-Slack Extension Testcase");

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setExtension("reorder:akslack", AlphaKSlackExtension.class);

        String inStreamDefinition = "define stream inputStream (eventtt long,data double);";
        String query = ("@info(name = 'query1') from inputStream#reorder:akslack(eventtt, " +
                "data, 20l) select  " +
                "eventtt, data " +
                "insert into outputStream;");

        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime
                (inStreamDefinition + query);

        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {

            @Override
            public void receive(io.siddhi.core.event.Event[] events) {

                for (io.siddhi.core.event.Event event : events) {
                    count.getAndIncrement();

                    if (count.get() == 1) {
                        AssertJUnit.assertEquals(1L, event.getData()[0]);
                    }

                    if (count.get() == 2) {
                        AssertJUnit.assertEquals(4L, event.getData()[0]);
                    }

                    if (count.get() == 3) {
                        AssertJUnit.assertEquals(3L, event.getData()[0]);
                    }

                    if (count.get() == 4) {
                        AssertJUnit.assertEquals(5L, event.getData()[0]);
                    }

                    if (count.get() == 5) {
                        AssertJUnit.assertEquals(6L, event.getData()[0]);
                    }

                    if (count.get() == 6) {
                        AssertJUnit.assertEquals(7L, event.getData()[0]);
                    }

                    if (count.get() == 7) {
                        AssertJUnit.assertEquals(8L, event.getData()[0]);
                    }

                    if (count.get() == 8) {
                        AssertJUnit.assertEquals(9L, event.getData()[0]);
                    }

                    if (count.get() == 9) {
                        AssertJUnit.assertEquals(10L, event.getData()[0]);
                    }

                    if (count.get() == 10) {
                        AssertJUnit.assertEquals(11L, event.getData()[0]);
                    }

                    if (count.get() == 11) {
                        AssertJUnit.assertEquals(12L, event.getData()[0]);
                    }

                    if (count.get() == 12) {
                        AssertJUnit.assertEquals(13L, event.getData()[0]);
                    }

                    if (count.get() == 13) {
                        AssertJUnit.assertEquals(14L, event.getData()[0]);
                    }

                    if (count.get() == 14) {
                        AssertJUnit.assertEquals(15L, event.getData()[0]);
                    }

                    if (count.get() == 15) {
                        AssertJUnit.assertEquals(16L, event.getData()[0]);
                    }

                    if (count.get() == 16) {
                        AssertJUnit.assertEquals(17L, event.getData()[0]);
                    }

                    if (count.get() == 17) {
                        AssertJUnit.assertEquals(18L, event.getData()[0]);
                    }

                    if (count.get() == 18) {
                        AssertJUnit.assertEquals(19L, event.getData()[0]);
                    }

                    if (count.get() == 19) {
                        AssertJUnit.assertEquals(20L, event.getData()[0]);
                    }

                    if (count.get() == 20) {
                        AssertJUnit.assertEquals(22L, event.getData()[0]);
                    }
                    if (count.get() == 21) {
                        AssertJUnit.assertEquals(21L, event.getData()[0]);
                    }
                    if (count.get() == 22) {
                        AssertJUnit.assertEquals(23L, event.getData()[0]);
                    }
                    if (count.get() == 23) {
                        AssertJUnit.assertEquals(24L, event.getData()[0]);
                    }
                    if (count.get() == 24) {
                        AssertJUnit.assertEquals(25L, event.getData()[0]);
                    }
                    if (count.get() == 25) {
                        AssertJUnit.assertEquals(26L, event.getData()[0]);
                    }
                    if (count.get() == 26) {
                        AssertJUnit.assertEquals(27L, event.getData()[0]);
                    }
                    if (count.get() == 27) {
                        AssertJUnit.assertEquals(29L, event.getData()[0]);
                    }
                    if (count.get() == 28) {
                        AssertJUnit.assertEquals(30L, event.getData()[0]);
                    }
                    if (count.get() == 29) {
                        AssertJUnit.assertEquals(31L, event.getData()[0]);
                    }
                    if (count.get() == 30) {
                        AssertJUnit.assertEquals(28L, event.getData()[0]);
                    }
                    if (count.get() == 31) {
                        AssertJUnit.assertEquals(32L, event.getData()[0]);
                    }
                    if (count.get() == 32) {
                        AssertJUnit.assertEquals(33L, event.getData()[0]);
                    }
                    if (count.get() == 33) {
                        AssertJUnit.assertEquals(34L, event.getData()[0]);
                    }

                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();

        inputHandler.send(new Object[]{1L, 79.0});
        inputHandler.send(new Object[]{4L, 60.0});
        inputHandler.send(new Object[]{3L, 65.0});
        inputHandler.send(new Object[]{5L, 30.0});
        inputHandler.send(new Object[]{6L, 43.0});
        inputHandler.send(new Object[]{9L, 90.0});
        inputHandler.send(new Object[]{7L, 98.7});
        inputHandler.send(new Object[]{8L, 80.0});
        inputHandler.send(new Object[]{10L, 100.0});
        inputHandler.send(new Object[]{12L, 19.0});
        inputHandler.send(new Object[]{13L, 45.0});
        inputHandler.send(new Object[]{14L, 110.0});
        inputHandler.send(new Object[]{11L, 92.0});
        inputHandler.send(new Object[]{15L, 29.0});
        inputHandler.send(new Object[]{17L, 55.0});
        inputHandler.send(new Object[]{18L, 61.0});
        inputHandler.send(new Object[]{19L, 33.0});
        inputHandler.send(new Object[]{16L, 30.0});
        inputHandler.send(new Object[]{20L, 66.0});
        inputHandler.send(new Object[]{22L, 42.0});
        inputHandler.send(new Object[]{23L, 61.0});
        inputHandler.send(new Object[]{24L, 33.0});
        inputHandler.send(new Object[]{25L, 30.0});
        inputHandler.send(new Object[]{26L, 14.0});
        inputHandler.send(new Object[]{21L, 42.0});
        inputHandler.send(new Object[]{27L, 45.0});
        inputHandler.send(new Object[]{29L, 29.0});
        inputHandler.send(new Object[]{30L, 9.0});
        inputHandler.send(new Object[]{31L, 13.0});
        inputHandler.send(new Object[]{33L, 66.0});
        inputHandler.send(new Object[]{28L, 29.0});
        inputHandler.send(new Object[]{34L, 51.0});
        inputHandler.send(new Object[]{32L, 27.0});


        SiddhiTestHelper.waitForEvents(waitTime, 33, count, timeout);
        executionPlanRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void testcase2() throws InterruptedException {
        log.info("Alpha K-Slack Extension Testcase invalid length");

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setExtension("reorder:akslack", AlphaKSlackExtension.class);

        String inStreamDefinition = "define stream inputStream (eventtt long);";
        String query = ("@info(name = 'query1') from inputStream#reorder:akslack(eventtt) select  "
                + "eventtt, data " + "insert into outputStream;");

        siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void testcase3() throws InterruptedException {
        log.info("Alpha K-Slack Extension Testcase invalid type first argument length two");

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setExtension("reorder:akslack", AlphaKSlackExtension.class);

        String inStreamDefinition = "define stream inputStream (eventtt string,data double);";
        String query = ("@info(name = 'query1') from inputStream#reorder:akslack(eventtt, data) select  "
                + "eventtt, data " + "insert into outputStream;");
        siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);

    }

    @Test
    public void testcase4() throws InterruptedException {
        log.info("Alpha K-Slack Extension Testcase invalid type second argument");

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setExtension("reorder:akslack", AlphaKSlackExtension.class);

        String inStreamDefinition = "define stream inputStream (eventtt long,data int);";
        String query = ("@info(name = 'query1') from inputStream#reorder:akslack(eventtt, data, 20l) select  "
                + "eventtt, data " + "insert into outputStream;");
        siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);

    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void testcase5() throws InterruptedException {
        log.info("Alpha K-Slack Extension Testcase for batchsize must be a constant length three");

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setExtension("reorder:akslack", AlphaKSlackExtension.class);

        String inStreamDefinition = "define stream inputStream (eventtt long,data double,data2 long);";
        String query = ("@info(name = 'query1') from inputStream#reorder:akslack(eventtt, data, data2 ) select  "
                + "eventtt, data " + "insert into outputStream;");

        siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void testcase6() throws InterruptedException {
        log.info("Alpha K-Slack Extension Testcase invalid type third argument length three");

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setExtension("reorder:akslack", AlphaKSlackExtension.class);

        String inStreamDefinition = "define stream inputStream (eventtt long,data double,data2 long);";
        String query = ("@info(name = 'query1') from inputStream#reorder:akslack(eventtt, "
                + "data,'batchSize' ) select  " + "eventtt, data " + "insert into outputStream;");

        siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void testcase7() throws InterruptedException {
        log.info("Alpha K-Slack Extension Testcase invalid parameter type forth argument length four");

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setExtension("reorder:akslack", AlphaKSlackExtension.class);

        String inStreamDefinition = "define stream inputStream (eventtt long,data double);";
        String query = ("@info(name = 'query1') from inputStream#reorder:akslack(eventtt, "
                + "data, 20l,'timerDuration' ) select  " + "eventtt, data " + "insert into outputStream;");

        siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void testcase8() throws InterruptedException {
        log.info("Alpha K-Slack Extension Testcase timerDuration must be a constant length four");

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setExtension("reorder:akslack", AlphaKSlackExtension.class);

        String inStreamDefinition = "define stream inputStream (eventtt long,data double,data1 long);";
        String query = ("@info(name = 'query1') from inputStream#reorder:akslack(eventtt, "
                + "data, 20l,data1) select  " + "eventtt, data " + "insert into outputStream;");

        siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void testcase9() throws InterruptedException {
        log.info("Alpha K-Slack Extension Testcase maxK must be a constant length five");

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setExtension("reorder:akslack", AlphaKSlackExtension.class);

        String inStreamDefinition = "define stream inputStream (eventtt long,data double,data1 long);";
        String query = ("@info(name = 'query1') from inputStream#reorder:akslack(eventtt, "
                + "data, 20l,12l,data1) select  " + "eventtt, data " + "insert into outputStream;");

        siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void testcase10() throws InterruptedException {
        log.info("Alpha K-Slack Extension Testcase invalid parameter fifth argument length five");

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setExtension("reorder:akslack", AlphaKSlackExtension.class);

        String inStreamDefinition = "define stream inputStream (eventtt long,data double,data1 long);";
        String query = ("@info(name = 'query1') from inputStream#reorder:akslack(eventtt, "
                + "data, 20l,12l,'maxK') select  " + "eventtt, data " + "insert into outputStream;");

        siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void testcase11() throws InterruptedException {
        log.info("Alpha K-Slack Extension Testcase invalid parameter length six");

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setExtension("reorder:akslack", AlphaKSlackExtension.class);

        String inStreamDefinition = "define stream inputStream (eventtt long,data double,data1 long);";
        String query = ("@info(name = 'query1') from inputStream#reorder:akslack(eventtt, "
                + "data, 20l,12l,10l,'discardFlag') select  " + "eventtt, data " + "insert into outputStream;");

        siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void testcase12() throws InterruptedException {
        log.info("Alpha K-Slack Extension Testcase discardFlag constant length six");

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setExtension("reorder:akslack", AlphaKSlackExtension.class);

        String inStreamDefinition = "define stream inputStream (eventtt long,data double,data1 long);";
        String query = ("@info(name = 'query1') from inputStream#reorder:akslack(eventtt, "
                + "data, 20l,12l,10l,data1) select  " + "eventtt, data1 " + "insert into outputStream;");

        siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void testcase13() throws InterruptedException {
        log.info("Alpha K-Slack Extension Testcase errorThreshold constant length eight");

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setExtension("reorder:akslack", AlphaKSlackExtension.class);

        String inStreamDefinition = "define stream inputStream (eventtt long,data double,data1 long,data2 long);";
        String query = ("@info(name = 'query1') from inputStream#reorder:akslack(eventtt, "
                + "data, 20l,12l,10l,true,data1,data2) select  " + "eventtt, data1 " + "insert into outputStream;");

        siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void testcase14() throws InterruptedException {
        log.info("Alpha K-Slack Extension Testcase invalid parameter seventh argument length eight");

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setExtension("reorder:akslack", AlphaKSlackExtension.class);

        String inStreamDefinition = "define stream inputStream (eventtt long,data double,data1 long);";
        String query = ("@info(name = 'query1') from inputStream#reorder:akslack(eventtt, "
                + "data, 20l,12l,10l,true,'errorThreshold',12) select  " + "eventtt, data1 "
                + "insert into outputStream;");

        siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void testcase15() throws InterruptedException {
        log.info("Alpha K-Slack Extension Testcase invalid parameter eighth argument length eight");

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setExtension("reorder:akslack", AlphaKSlackExtension.class);

        String inStreamDefinition = "define stream inputStream (eventtt long,data double,data1 long);";
        String query = ("@info(name = 'query1') from inputStream#reorder:akslack(eventtt, "
                + "data, 20l,12l,10l,true,12.5,'confidenceLevel') select  " + "eventtt, data1 "
                + "insert into outputStream;");

        siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
    }

    @Test
    public void testcase16() throws InterruptedException, CannotRestoreSiddhiAppStateException {
        log.info("Alpha K-Slack Extension Testcase for current state & restore state");

        PersistenceStore persistenceStore = new InMemoryPersistenceStore();
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);
        siddhiManager.setExtension("reorder:akslack", AlphaKSlackExtension.class);

        String inStreamDefinition = "define stream inputStream (eventtt long,data double);";
        String query = ("@info(name = 'query1') from inputStream#reorder:akslack(eventtt, " + "data, 20l) select  "
                + "eventtt, data " + "insert into outputStream;");

        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {

            @Override
            public void receive(io.siddhi.core.event.Event[] events) {

                for (io.siddhi.core.event.Event event : events) {
                    count.getAndIncrement();

                    if (count.get() == 1) {
                        AssertJUnit.assertEquals(1L, event.getData()[0]);
                    }

                    if (count.get() == 2) {
                        AssertJUnit.assertEquals(4L, event.getData()[0]);
                    }

                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();

        inputHandler.send(new Object[] { 1L, 79.0 });
        //persisting
        SiddhiTestHelper.waitForEvents(waitTime, 1, count, timeout);
        executionPlanRuntime.persist();
        //restarting execution plan
        executionPlanRuntime.shutdown();
        inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        //loading
        executionPlanRuntime.restoreLastRevision();
        inputHandler.send(new Object[] { 4L, 60.0 });
        SiddhiTestHelper.waitForEvents(waitTime, 1, count, timeout);
        executionPlanRuntime.shutdown();
    }
}
