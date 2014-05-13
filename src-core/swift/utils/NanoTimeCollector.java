/*****************************************************************************
 * Copyright 2011-2012 INRIA
 * Copyright 2011-2012 Universidade Nova de Lisboa
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
 *****************************************************************************/
package swift.utils;

/**
 * Measuring time between two events.
 * 
 * @author annettebieniusa
 * 
 */
public class NanoTimeCollector {

    private long time;
    private long duration;
    private long total;
    private long iterations;

    public NanoTimeCollector() {
        duration = 0;
        time = 0;
        iterations = 0;
        total = 0;
    }

    /**
     * Starts the timer
     */
    public void start() {
        time = System.currentTimeMillis();
    }

    /**
     * Returns the duration since last timer start. Additionally, total duration
     * gets accumulated, and number of iterations increased.
     * 
     * @return duration since last timer start
     */
    public long stop() {
        duration = System.currentTimeMillis() - time;
        total += duration;
        iterations++;
        return duration;
    }

    /**
     * Resets the total duration.
     */
    public void reset() {
        duration = 0;
        iterations = 0;
        total = 0;
    }

    /**
     * Returns the accumulated duration of all measured timer intervals since
     * the creation of timer or the last timer reset.
     * 
     * @return accumulated duration of all measured timer intervals
     */
    public long getTotalDuration() {
        return total;
    }

    /**
     * Returns the average of all time intervals measured since creating of
     * timer or the last timer reset.
     * 
     * It is often a good idea to reset the timer after several iterations to
     * exclude "bad" intervals due to system setup, just-in-time compilation
     * etc.
     * 
     * @return average duration of all measured timer intervals
     */
    public long getAverage() {
        return total / iterations;
    }
}
