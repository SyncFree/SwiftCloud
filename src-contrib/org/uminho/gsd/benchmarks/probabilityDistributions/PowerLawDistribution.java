/*
 * *********************************************************************
 * Copyright (c) 2010 Pedro Gomes and Universidade do Minho.
 * All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * ********************************************************************
 */

package org.uminho.gsd.benchmarks.probabilityDistributions;


import cern.jet.random.Distributions;
import org.apache.log4j.Logger;
import org.uminho.gsd.benchmarks.benchmark.BenchmarkMain;
import org.uminho.gsd.benchmarks.interfaces.ProbabilityDistribution;

import java.util.Map;
import java.util.TreeMap;

public class PowerLawDistribution implements ProbabilityDistribution {

    private int size = 0;

    private Logger logger = Logger.getLogger(PowerLawDistribution.class);

    /**
     * Default alpha value
     */
    private double default_alpha = 2;

    /**
     * Number Generator
     */
    private cern.jet.random.engine.RandomEngine generator;

    /**
     * Added info
     */
    Map<String, String> info;

    /**
     * Distribution alpha factor
     */
    private double alpha;

    public PowerLawDistribution() {


    }

    public PowerLawDistribution(int size, double alpha) {
        generator = cern.jet.random.AbstractDistribution.makeDefaultGenerator();
        this.size = size;
        this.alpha = alpha;

    }

    public String getName() {
        return "Power Law Distribution";  //To change body of implemented methods use File | Settings | File Templates.
    }

    public Map<String, String> getInfo() {
        if (info == null)
            return new TreeMap<String, String>();

        if(BenchmarkMain.distribution_factor!=-1){
           info.put("skew",this.alpha+"");
        }

        return info;
    }

    public void init(int numberElements, Map<String, Object> options) {
        generator = cern.jet.random.AbstractDistribution.makeDefaultGenerator();
        this.size = numberElements;
        alpha = default_alpha;

        if(BenchmarkMain.distribution_factor!=-1){
            logger.warn("Power law factor set to user defined level: " +BenchmarkMain.distribution_factor);
            alpha = BenchmarkMain.distribution_factor;
            return;
        }

        if (info == null || !info.containsKey("alpha")) {
            System.out.println("[WARN:] ALPHA OPTION IS NOT DEFINED IN USED POWER LAW DISTRIBUTION. DEFAULT: 2");
        } else {
            this.alpha = Double.parseDouble(info.get("alpha").trim());
            if (alpha == Double.NaN) {
                alpha = default_alpha;
                logger.warn("Power law factor set to default due to error:" + default_alpha);
            }
        }
    }

    public void setInfo(Map<String, String> info) {
        this.info = info;
    }

    public int getNextElement() {
        return (int) Distributions.nextPowLaw(alpha, size, generator);
    }

    public ProbabilityDistribution getNewInstance() {
        return new PowerLawDistribution(size, alpha); //To change body of implemented methods use File | Settings | File Templates.
    }
}
