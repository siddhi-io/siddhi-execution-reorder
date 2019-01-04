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

import org.apache.commons.math3.distribution.NormalDistribution;
import org.wso2.extension.siddhi.execution.reorder.utils.WindowCoverage;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.ReturnAttribute;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.SchedulingProcessor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.core.util.Scheduler;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;


/**
 * The following code conducts reordering of an out-of-order event stream.
 * This implements the Alpha K-Slack based disorder handling algorithm which was originally
 * described in http://dl.acm.org/citation.cfm?doid=2675743.2771828
 */

@Extension(
        name = "akslack",
        namespace = "reorder",
        description = "This stream processor extension performs reordering of an out-of-order event stream.\n" +
                " It implements the AQ-K-Slack based out-of-order handling algorithm (originally described in \n" +
                "http://dl.acm.org/citation.cfm?doid=2675743.2771828)",
        parameters = {
                @Parameter(name = "timestamp",
                        description = "Attribute used for ordering the events",
                        type = {DataType.LONG}),
                @Parameter(name = "correlation.field",
                        description = "Corresponds to the data field of which the accuracy directly gets affected " +
                                "by the adaptive operation of the Alpha K-Slack extension. This field is used by the " +
                                "Alpha K-Slack to calculate the runtime window coverage threshold which is an upper " +
                                "limit set for the unsuccessfully handled late arrivals",
                        type = {DataType.INT, DataType.FLOAT, DataType.LONG, DataType.DOUBLE}),
                @Parameter(name = "batch.size",
                        description = "The parameter batch.size denotes the number of events that should be " +
                                "considered in the calculation of an alpha value. batch.size should be a value " +
                                "which should be greater than or equals to 15",
                        defaultValue = "10,000",
                        type = {DataType.LONG},
                        optional = true),
                @Parameter(name = "timer.timeout",
                        description = "Corresponds to a fixed time-out value in milliseconds, which is set at " +
                                "the beginning of the process. " +
                                "Once the time-out value expires, the extension drains all the events that are " +
                                "buffered within the reorder extension to outside. The time out has been implemented " +
                                "internally using a timer. The events buffered within the extension are released " +
                                "each time the timer ticks.",
                        defaultValue = "-1 (timeout is infinite)",
                        type = {DataType.LONG},
                        optional = true),
                @Parameter(name = "max.k",
                        description = "The maximum threshold value for K parameter in the Alpha K-Slack algorithm",
                        defaultValue = "9,223,372,036,854,775,807 (The maximum Long value)",
                        type = {DataType.LONG},
                        optional = true),
                @Parameter(name = "discard.flag",
                        description = "Indicates whether the out-of-order events which appear after the expiration " +
                                "of the Alpha K-slack window should get discarded or not. When this value is set " +
                                "to true, the events would get discarded",
                        defaultValue = "false",
                        type = {DataType.BOOL},
                        optional = true),
                @Parameter(name = "error.threshold",
                        description = "Error threshold to be applied in Alpha K-Slack algorithm. This parameter must " +
                                "be defined simultaneously with confidenceLevel",
                        defaultValue = "0.03 (3%)",
                        type = {DataType.DOUBLE},
                        optional = true),
                @Parameter(name = "confidence.level",
                        description = "Confidence level to be applied in Alpha K-Slack algorithm. This parameter " +
                                "must be defined simultaneously with errorThreshold",
                        defaultValue = "0.95 (95%)",
                        type = {DataType.DOUBLE},
                        optional = true)
        },
        returnAttributes = {
                @ReturnAttribute(
                        name = "beta0",
                        description = "Timestamp based on which the reordering is performed",
                        type = {DataType.LONG}
                ),
                @ReturnAttribute(
                        name = "beta1",
                        description = "An upper limit value assigned for the unsuccessfully handled late arrivals",
                        type = {DataType.DOUBLE}
                ),
                @ReturnAttribute(
                        name = "beta2",
                        description = "The number of events that should be considered in the calculation of an alpha " +
                                "value",
                        type = {DataType.LONG}
                ),
                @ReturnAttribute(
                        name = "beta3",
                        description = "Fixed time-out value (in milliseconds) assigned for flushing all the events " +
                                "buffered inside the extension.",
                        type = {DataType.LONG}
                ),
                @ReturnAttribute(
                        name = "beta4",
                        description = "Maximum threshold value assigned for K parameter.",
                        type = {DataType.LONG}
                ),
                @ReturnAttribute(
                        name = "beta5",
                        description = "Flag set to indicate whether out-of-order events which arrive after buffer " +
                                "eviction to be discarded or not",
                        type = {DataType.BOOL}
                ),
                @ReturnAttribute(
                        name = "beta6",
                        description = "Error threshold value set for Alpha K-Slack algorithm",
                        type = {DataType.DOUBLE}
                ),
                @ReturnAttribute(
                        name = "beta7",
                        description = "Confidence level set for the Alpha K-Slack algorithm",
                        type = {DataType.DOUBLE}
                )
        },
        examples = @Example(
                syntax = "define stream inputStream (eventtt long,data double);\n" +
                        "@info(name = 'query1')\n" +
                        "from inputStream#reorder:akslack(eventtt, data, 20)\n" +
                        "select eventtt, data\n" +
                        "insert into outputStream;",
                description = "This query performs reordering based on the 'eventtt' attribute values. In this " +
                        "example, 20 represents the batch size")
)
public class AlphaKSlackExtension extends StreamProcessor implements SchedulingProcessor {
    private Long k = 0L; //In the beginning the K is zero.
    private Long largestTimestamp = 0L; //Used to track the greatest timestamp of tuples seen so far.
    private TreeMap<Long, List<StreamEvent>> primaryTreeMap;
    private TreeMap<Long, List<StreamEvent>> secondaryTreeMap;
    private ExpressionExecutor timestampExecutor;
    private ExpressionExecutor correlationFieldExecutor;
    private Long maxK = Long.MAX_VALUE;
    private Long timerDuration = -1L;
    private boolean discardFlag = false;
    private Long lastSentTimestamp = -1L;
    private Scheduler scheduler;
    private Long lastScheduledTimestamp = -1L;
    private ReentrantLock lock = new ReentrantLock();
    private double previousAlpha = 0;
    private Integer counter = 0;
    private Long batchSize = 10000L;
    private double previousError = 0;
    private List<Double> dataItemList = new ArrayList<Double>();
    private List<Long> timestampList = new ArrayList<Long>();
    private double kp = 0.5; // Weight configuration parameters
    private double kd = 0.8;
    private boolean flag = true;
    private boolean timerFlag = true;
    private double errorThreshold = 0.03;
    private double confidenceLevel = 0.95;
    private double alpha = 1;
    private SiddhiAppContext siddhiAppContext;
    private WindowCoverage windowCoverage;
    private double criticalValue;
    private long l = 0;
    private long windowSize = 10000000000L;

    public AlphaKSlackExtension() {
    }

    @Override
    public void start() {
        //Do nothing
    }

    @Override
    public void stop() {
        //Do nothing
    }

    @Override
    public Map<String, Object> currentState() {
        Map<String, Object> stateMap = new HashMap<String, Object>();
        stateMap.put("k", k);
        stateMap.put("largestTimestamp", largestTimestamp);
        stateMap.put("lastSentTimestamp", lastSentTimestamp);
        stateMap.put("lastScheduledTimestamp", lastScheduledTimestamp);
        stateMap.put("previousAlpha", previousAlpha);
        stateMap.put("counter", counter);
        stateMap.put("previousError", previousError);
        stateMap.put("kp", kp);
        stateMap.put("kd", kd);
        stateMap.put("primaryTreeMap", primaryTreeMap);
        stateMap.put("secondaryTreeMap", secondaryTreeMap);
        stateMap.put("dataItemList", dataItemList);
        stateMap.put("timestampList", timestampList);
        return stateMap;
    }

    @Override
    public void restoreState(Map<String, Object> map) {
        k = (Long) map.get("k");
        largestTimestamp = (Long) map.get("largestTimestamp");
        lastSentTimestamp = (Long) map.get("lastSentTimestamp");
        lastScheduledTimestamp = (Long) map.get("lastScheduledTimestamp");
        previousAlpha = (Double) map.get("previousAlpha");
        counter = (Integer) map.get("counter");
        previousError = (Double) map.get("previousError");
        kp = (Double) map.get("kp");
        kd = (Double) map.get("kd");
        primaryTreeMap = (TreeMap) map.get("primaryTreeMap");
        secondaryTreeMap = (TreeMap) map.get("secondaryTreeMap");
        dataItemList = (List) map.get("dataItemList");
        timestampList = (List) map.get("timestampList");
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner,
                           ComplexEventPopulater complexEventPopulater) {
        ComplexEventChunk<StreamEvent> complexEventChunk = new ComplexEventChunk<StreamEvent>(false);
        try {
            lock.lock();
            while (streamEventChunk.hasNext()) {
                StreamEvent event = streamEventChunk.next();

                if (event.getType() != ComplexEvent.Type.TIMER) {
                    streamEventChunk.remove();
                    long timestamp = (Long) timestampExecutor.execute(event);
                    timestampList.add(timestamp);
                    double correlationField;
                    if (correlationFieldExecutor.getReturnType() == Attribute.Type.INT) {
                        correlationField = (Integer) correlationFieldExecutor.execute(event);
                    } else {
                        correlationField = (Double) correlationFieldExecutor.execute(event);
                    }
                    dataItemList.add(correlationField);
                    if (discardFlag) {
                        if (timestamp < lastSentTimestamp) {
                            continue;
                        }
                    }

                    if (timerFlag) {
                        timerFlag = false;
                        lastScheduledTimestamp = lastScheduledTimestamp + timerDuration;
                        scheduler.notifyAt(lastScheduledTimestamp);
                    }

                    List<StreamEvent> eventList = primaryTreeMap.computeIfAbsent(timestamp, k1 -> new ArrayList<>());
                    eventList.add(event);
                    counter += 1;
                    if (counter > batchSize) {
                        if (l == 0) {
                            alpha = calculateAlpha(windowCoverage.calculateWindowCoverageThreshold(criticalValue,
                                    dataItemList), 1);
                            l = Math.round(alpha * k);
                            if (l > k) {
                                l = k;
                            }
                        } else {
                            alpha = calculateAlpha(windowCoverage.calculateWindowCoverageThreshold(criticalValue,
                                    dataItemList),
                                    windowCoverage.calculateRuntimeWindowCoverage(timestamp, timestampList,
                                            l, windowSize));
                            l = Math.round(alpha * k);
                            if (l > k) {
                                l = k;
                            }
                        }
                        counter = 0;
                        dataItemList.clear();
                    }
                    if (timestamp > largestTimestamp) {
                        largestTimestamp = timestamp;
                        long minTimestamp = primaryTreeMap.firstKey();
                        long timeDifference = largestTimestamp - minTimestamp;
                        if (timeDifference > k) {
                            if (timeDifference < maxK) {
                                k = Math.round(timeDifference * alpha);
                            } else {
                                k = maxK;
                            }
                        }

                        Iterator<Map.Entry<Long, List<StreamEvent>>> entryIterator =
                                primaryTreeMap.entrySet()
                                        .iterator();
                        while (entryIterator.hasNext()) {
                            Map.Entry<Long, List<StreamEvent>> entry = entryIterator.next();
                            List<StreamEvent> list = secondaryTreeMap.get(entry.getKey());
                            if (list != null) {
                                list.addAll(entry.getValue());
                            } else {
                                secondaryTreeMap.put(entry.getKey(),
                                        new ArrayList<>(entry.getValue()));
                            }
                        }
                        primaryTreeMap.clear();
                        entryIterator = secondaryTreeMap.entrySet().iterator();
                        while (entryIterator.hasNext()) {
                            Map.Entry<Long, List<StreamEvent>> entry = entryIterator.next();
                            if (entry.getKey() + k <= largestTimestamp) {
                                entryIterator.remove();
                                List<StreamEvent> timeEventList = entry.getValue();
                                lastSentTimestamp = entry.getKey();

                                for (StreamEvent aTimeEventList : timeEventList) {
                                    complexEventChunk.add(aTimeEventList);
                                }
                            } else {
                                break;
                            }
                        }
                    }
                } else {
                    if (timerDuration != -1) {
                        if (secondaryTreeMap.size() > 0) {
                            for (Map.Entry<Long, List<StreamEvent>> longListEntry :
                                    secondaryTreeMap.entrySet()) {
                                List<StreamEvent> timeEventList = longListEntry.getValue();

                                for (StreamEvent aTimeEventList : timeEventList) {
                                    complexEventChunk.add(aTimeEventList);
                                }
                            }

                            secondaryTreeMap = new TreeMap<Long, List<StreamEvent>>();

                        }

                        if (primaryTreeMap.size() > 0) {
                            for (Map.Entry<Long, List<StreamEvent>> longListEntry :
                                    primaryTreeMap.entrySet()) {
                                List<StreamEvent> timeEventList = longListEntry.getValue();

                                for (StreamEvent aTimeEventList : timeEventList) {
                                    complexEventChunk.add(aTimeEventList);
                                }
                            }

                            primaryTreeMap = new TreeMap<Long, List<StreamEvent>>();
                        }

                        timerFlag = true;
                    }
                }
            }
        } catch (ArrayIndexOutOfBoundsException ec) {
            //This happens due to user specifying an invalid field index.
            throw new SiddhiAppCreationException("The very first parameter must be an " +
                    "Integer with a valid " +
                    " field index (0 to (fieldsLength-1)).");
        } finally {
            lock.unlock();
        }

        nextProcessor.process(complexEventChunk);
    }

    @Override
    protected List<Attribute> init(AbstractDefinition abstractDefinition, ExpressionExecutor[] expressionExecutors,
                                   ConfigReader configReader, SiddhiAppContext siddhiAppContext) {
        List<Attribute> attributes = new ArrayList<Attribute>();
        this.siddhiAppContext = siddhiAppContext;
        if (attributeExpressionLength > 8 || attributeExpressionLength < 2
                || attributeExpressionLength == 7) {
            throw new SiddhiAppCreationException("Maximum six input parameters " +
                    "and minimum two input parameters " +
                    "can be specified for AK-Slack. " +
                    " Timestamp (long), velocity (long), batchSize (long), "
                    + "timerTimeout "
                    +
                    "(long), maxK (long), " +
                    "discardFlag (boolean), errorThreshold (double) and "
                    + "confidenceLevel "
                    +
                    "(double)  fields. But found " +
                    attributeExpressionLength + " attributes.");
        }

        if (attributeExpressionExecutors.length >= 2) {
            flag = false;
            if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.LONG) {
                timestampExecutor = attributeExpressionExecutors[0];
                attributes.add(new Attribute("beta0", Attribute.Type.LONG));
            } else {
                throw new SiddhiAppCreationException("Invalid parameter type found for " +
                        "the first argument of " +
                        " reorder:akslack() function. Required LONG, but "
                        + "found "
                        +
                        attributeExpressionExecutors[0].getReturnType());
            }

            switch (attributeExpressionExecutors[1].getReturnType()) {
                case INT:
                case LONG:
                case FLOAT:
                case DOUBLE:
                    correlationFieldExecutor = attributeExpressionExecutors[1];
                    attributes.add(new Attribute("beta1", Attribute.Type.DOUBLE));
                    break;
                case BOOL:
                case OBJECT:
                case STRING:
                    throw new SiddhiAppCreationException("Invalid parameter type found for " +
                            "the second argument of reorder:akslack() function. Required INT, " +
                            "FLOAT, DOUBLE, or LONG but found " + attributeExpressionExecutors[1].getReturnType());
            }
        }
        if (attributeExpressionExecutors.length >= 3) {
            if (attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.LONG) {
                    attributes.add(new Attribute("beta2", Attribute.Type.LONG));
                    batchSize = (Long) ((ConstantExpressionExecutor)
                            attributeExpressionExecutors[2]).getValue();
                } else {
                    throw new SiddhiAppCreationException("Invalid parameter type found " +
                            "for the third argument of " +
                            " reorder:akslack() function. Required LONG, but"
                            + " found "
                            +
                            attributeExpressionExecutors[2].getReturnType());
                }
            } else {
                throw new SiddhiAppCreationException("Batch size parameter must be a constant.");
            }

        }
        if (attributeExpressionExecutors.length >= 4) {
            flag = true;
            if (attributeExpressionExecutors[3] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[3].getReturnType() == Attribute.Type.LONG) {
                    timerDuration = (Long) ((ConstantExpressionExecutor)
                            attributeExpressionExecutors[3]).getValue();
                    attributes.add(new Attribute("beta3", Attribute.Type.LONG));
                } else {
                    throw new SiddhiAppCreationException("Invalid parameter type found for " +
                            "the fourth argument of " +
                            " reorder:akslack() function. Required LONG, but"
                            + " found "
                            +
                            attributeExpressionExecutors[3].getReturnType());
                }
            } else {
                throw new SiddhiAppCreationException("timerDuration must be a constant");
            }
        }
        if (attributeExpressionExecutors.length >= 5) {
            if (attributeExpressionExecutors[4] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[4].getReturnType() == Attribute.Type.LONG) {
                    maxK = (Long) ((ConstantExpressionExecutor)
                            attributeExpressionExecutors[4]).getValue();
                    if (maxK == -1) {
                        maxK = Long.MAX_VALUE;
                    }
                    attributes.add(new Attribute("beta4", Attribute.Type.LONG));
                } else {
                    throw new SiddhiAppCreationException("Invalid parameter type found " +
                            "for the fifth argument of " +
                            " reorder:akslack() function. Required LONG, but"
                            + " found "
                            +
                            attributeExpressionExecutors[4].getReturnType());
                }
            } else {
                throw new SiddhiAppCreationException("maxK must be a constant");
            }
        }
        if (attributeExpressionExecutors.length >= 6) {
            if (attributeExpressionExecutors[5] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[5].getReturnType() == Attribute.Type.BOOL) {
                    discardFlag = (Boolean) attributeExpressionExecutors[5].execute(null);
                    attributes.add(new Attribute("beta5", Attribute.Type.BOOL));
                } else {
                    throw new SiddhiAppCreationException("Invalid parameter type found " +
                            "for the sixth argument of " +
                            " reorder:akslack() function. Required BOOL, but"
                            + " found "
                            +
                            attributeExpressionExecutors[5].getReturnType());
                }
            } else {
                throw new SiddhiAppCreationException("discardFlag must be a constant");
            }
        }
        if (attributeExpressionExecutors.length == 8) {
            if ((attributeExpressionExecutors[6] instanceof ConstantExpressionExecutor) &&
                    attributeExpressionExecutors[7] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[6].getReturnType() == Attribute.Type.DOUBLE) {
                    errorThreshold = (Double) ((ConstantExpressionExecutor)
                            attributeExpressionExecutors[6]).getValue();
                    attributes.add(new Attribute("beta6", Attribute.Type.DOUBLE));
                } else {
                    throw new SiddhiAppCreationException("Invalid parameter type found " +
                            "for the seventh argument of " +
                            " reorder:akslack() function. Required DOUBLE, "
                            + "but found "
                            +
                            attributeExpressionExecutors[6].getReturnType());
                }
                if (attributeExpressionExecutors[7].getReturnType() == Attribute.Type.DOUBLE) {
                    confidenceLevel = (Double) ((ConstantExpressionExecutor)
                            attributeExpressionExecutors[7]).getValue();
                    attributes.add(new Attribute("beta7", Attribute.Type.DOUBLE));
                } else {
                    throw new SiddhiAppCreationException("Invalid parameter type found for " +
                            "the eighth argument of " +
                            " reorder:akslack() function. Required DOUBLE, "
                            + "but found "
                            + attributeExpressionExecutors[7].getReturnType());
                }
            } else {
                throw new SiddhiAppCreationException("errorThreshold and " +
                        "confidenceLevel must be constants");
            }
        }
        NormalDistribution actualDistribution = new NormalDistribution();
        criticalValue = Math.abs(actualDistribution.inverseCumulativeProbability
                ((1 - confidenceLevel) / 2));
        windowCoverage = new WindowCoverage(errorThreshold);
        primaryTreeMap = new TreeMap<Long, List<StreamEvent>>();
        secondaryTreeMap = new TreeMap<Long, List<StreamEvent>>();

        return attributes;
    }

    @Override
    public Scheduler getScheduler() {
        return this.scheduler;
    }

    @Override
    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
        if (lastScheduledTimestamp < 0 && flag) {
            lastScheduledTimestamp = siddhiAppContext.getTimestampGenerator().currentTime() +
                    timerDuration;
            scheduler.notifyAt(lastScheduledTimestamp);
        }
    }

    private double calculateAlpha(double windowCoverageThreshold, double runtimeWindowCoverage) {
        double error = windowCoverageThreshold - runtimeWindowCoverage;
        double deltaAlpha = (kp * error) + (kd * (error - previousError));
        double alpha = Math.abs(previousAlpha + deltaAlpha);
        previousError = error;
        previousAlpha = alpha;
        return alpha;
    }
}
