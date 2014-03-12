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
package sys.stats.output;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import sys.stats.StatsImpl;
import sys.stats.StatsConstants;
import sys.stats.common.PlaneValue;
import sys.stats.common.PlotValues;
import sys.stats.sliced.slices.histogram.Histogram;

/**
 * Dumps statistics to files
 * 
 * @author balegas
 * 
 */

public class BufferedFileDumper implements StatisticsOutput {

    private String filename;
    private OutputStream output;
    private static Logger logger = Logger.getLogger(StatsImpl.class.getName());

    public BufferedFileDumper(String filename) {
        this.filename = filename;
    }

    public void init() throws FileNotFoundException {
        File file = new File(filename);
        output = new BufferedOutputStream(new FileOutputStream(file));
    }

    private boolean checkInit() {
        if (output == null) {
            logger.log(Level.WARNING, "Trying to output statistics but channel is not initialized");
            return false;
        }
        return true;

    }

    /**
     * Outputs the last slice of collected data
     */
    public void outputCurrent() {
        if (!checkInit())
            return;
        // TODO: Not implemented
    }

    /**
     * Outputs all slices of collected data for a given ValueOutput
     * 
     * @param valuesOutput
     *            the collected values
     */
    @Override
    public void output(ValuesOutput valuesOutput) throws IOException {
        if (!checkInit()) {
            return;
        }

        PlotValues plotValues = valuesOutput.getPlotValues();
        Iterator<PlaneValue> it = plotValues.getPlotValuesIterator();
        while (it.hasNext()) {
            PlaneValue value = it.next();
            output.write(("" + value.getX() + StatsConstants.VS + value.getY() + "\n").getBytes());
        }
    }

    /**
     * Outputs all slices of collected data for a given ValueOutput
     * 
     * @param valuesOutput
     *            the collected values
     */
    @Override
    public void output(HistogramOutput valuesOutput) throws IOException {
        if (!checkInit()) {
            return;
        }

        String s = valuesOutput.getHistogramBinValues();
        PlotValues<Long, Histogram> plotValues = valuesOutput.getPlotValues();
        Iterator<PlaneValue<Long, Histogram>> it = plotValues.getPlotValuesIterator();
        output.write((s + "\n").getBytes());
        while (it.hasNext()) {
            PlaneValue<Long, Histogram> value = it.next();
            output.write((value.getX() + " " + value.getY() + "\n").getBytes());
        }
    }

    /**
     * Closes the output stream
     */
    @Override
    public void close() throws IOException {
        output.close();

    }

}
