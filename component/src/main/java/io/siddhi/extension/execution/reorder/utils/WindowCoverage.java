/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package io.siddhi.extension.execution.reorder.utils;

import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * This class calculate the window coverage
 */
public class WindowCoverage {
    private static final Logger log = Logger.getLogger(WindowCoverage.class);
    private double errorThreshold;

    public WindowCoverage(double errorThreshold) {
        this.errorThreshold = errorThreshold;
    }

    /**
     * The following method - calculateWindowCoverageThreshold returns window coverage threshold
     * which is found as a minimum value that satisfying two inequalities in the form of :
     *              a1(x^2) + b1(x) + c1 <= 0    and
     *              a1(x^2) + b2(x) + c2 <=0
     */

    /**
     * Calculate Window Coverage Threshold
     *
     * @param criticalValue critical value for algorithm
     * @return windowCoverageThreshold
     */
    public double calculateWindowCoverageThreshold(double criticalValue, List<Double> listOfData) {
        double mean = calculateMean(listOfData);
        double variance = calculateVariance(listOfData, mean);
        double temp1 = Math.sqrt((Math.pow(mean, 2) + Math.pow(variance, 2)) /
                (listOfData.size() * Math.pow(mean, 2)));
        double temp2 = Math.pow(criticalValue, 2) * Math.pow(temp1, 2);
        double a1, b1, c1, b2, c2;
        double windowCoverageThreshold;
        double theta1, theta2, theta3, theta4, tempSq1, tempSq2;
        a1 = 1 + temp2;
        b1 = (2 * errorThreshold) - 2 - temp2;
        c1 = Math.pow((1 - errorThreshold), 2);
        b2 = -(2 + (2 * errorThreshold) + temp2);
        c2 = Math.pow((1 + errorThreshold), 2);
        tempSq1 = Math.pow(b1, 2) - (4 * a1 * c1);
        tempSq2 = Math.pow(b2, 2) - (4 * a1 * c2);

        if (tempSq1 >= 0) {
            theta1 = (-b1 - Math.sqrt(tempSq1)) / (2 * a1);
            theta2 = (-b1 + Math.sqrt(tempSq1)) / (2 * a1);
        } else {
            theta1 = theta2 = 0;
        }
        if (tempSq2 >= 0) {
            theta3 = (-b2 - Math.sqrt(tempSq2)) / (2 * a1);
            theta4 = (-b2 + Math.sqrt(tempSq2)) / (2 * a1);
        } else {
            theta3 = theta4 = 0;
        }
        if ((tempSq1 >= 0) && (tempSq2 < 0)) {
            windowCoverageThreshold = Math.min(theta1, theta2);
        } else if ((tempSq2 >= 0) && (tempSq1 < 0)) {
            windowCoverageThreshold = Math.min(theta3, theta4);
        } else if ((tempSq1 < 0) && (tempSq2 < 0)) {
            windowCoverageThreshold = 0;
        } else {
            windowCoverageThreshold = Math.min(Math.min(theta1, theta2), Math.min(theta3, theta4));
        }
        return windowCoverageThreshold;
    }

    private double calculateMean(List<Double> listOfData) {
        double sum = 0;
        for (Double aListOfData : listOfData) {
            sum += aListOfData;
        }
        return sum / listOfData.size();
    }

    private double calculateVariance(List<Double> listOfData, double meanValue) {
        double squaredSum = 0;
        for (Double aListOfData : listOfData) {
            squaredSum += Math.pow((meanValue - aListOfData), 2);
        }
        return squaredSum / listOfData.size();
    }

    /**
     * Calculate Window Coverage
     *
     * @param eventTimestamps Timestamp of events in window
     * @param windowSize      Size of window
     * @return runtimeWindowCoverage
     */
    public double calculateRuntimeWindowCoverage(List<Long> eventTimestamps, long windowSize) {
//        long start = System.currentTimeMillis();
        double runtimeWindowCoverage = -1;
        int numerator = 0;
        int denominator = 0;
        long lowerIndex = 0;
        long timestamp;
        Iterator<Long> timestampEntryIterator = eventTimestamps.iterator();
        long largestTimestamp = Collections.max(eventTimestamps);
        int indexOfLargestTimestamp = eventTimestamps.indexOf(largestTimestamp);
        long edgeValue = (largestTimestamp - windowSize);
        if (timestampEntryIterator.hasNext()) {
            timestamp = timestampEntryIterator.next();
            long distance = Math.abs(timestamp - edgeValue);
            while (timestampEntryIterator.hasNext()) {
                timestamp = timestampEntryIterator.next();
                long cdistance = Math.abs(timestamp - edgeValue);
                if (cdistance < distance) {
                    distance = cdistance;
                    lowerIndex = eventTimestamps.indexOf(timestamp);
                }
            }
            for (long i = edgeValue; i <= largestTimestamp; i++) {
                int z = eventTimestamps.indexOf(i);
                if (z >= 0) {
                    if ((z <= indexOfLargestTimestamp) && (z >= lowerIndex)) {
                        numerator += 1;
                    }
                    denominator += 1;
                }
            }
            runtimeWindowCoverage = numerator * 1.0 / denominator;
        }
//        log.info("Runtime window coverage: " + (System.currentTimeMillis() - start));
        return runtimeWindowCoverage;
    }

    public double calculateRuntimeWindowCoverage(long currentTimestamp, List<Long> timestamps,
                                                 long l, long windowSize) {
        long windowHigh = currentTimestamp - l;
        long windowLow = currentTimestamp - l - windowSize;
        int coveredInWindow = 0;
        int totalEventsForWindow = 0;
        boolean lowWindowTimeMet = false;
        boolean highWindowTimeMet = false;
        Iterator<Long> timestampIterator = timestamps.iterator();
        while (timestampIterator.hasNext()) {
            long timestamp = timestampIterator.next();
            if (!lowWindowTimeMet && windowLow <= timestamp) {
                lowWindowTimeMet = true;
            }
            if (!highWindowTimeMet && windowHigh <= timestamp) {
                highWindowTimeMet = true;
            }
            if (lowWindowTimeMet) {
                if (windowLow <= timestamp && windowHigh >= timestamp) {
                    if (!highWindowTimeMet) {
                        coveredInWindow++;
                    }
                    totalEventsForWindow++;
                }
            }
            if (timestamp <= windowLow) {
                timestampIterator.remove();
            }
        }
        if (totalEventsForWindow != 0) {
            return (coveredInWindow / (double) totalEventsForWindow);
        } else {
            return 1.0;
        }
    }
}
