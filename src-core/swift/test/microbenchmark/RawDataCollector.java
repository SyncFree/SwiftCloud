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
package swift.test.microbenchmark;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class RawDataCollector {

    // [0] -> timeToexecute, [1] -> txType, [2] -> opCount, [4]
    // -> startTime
    int runCount;
    private String workerName;
    private String outputDir;
    private PrintWriter pw;
    private FileOutputStream fos;


    public RawDataCollector(int initialSize, String workerName, int runCount, String outputDir) {
        this.workerName = workerName;
        this.runCount = runCount;
        this.outputDir = outputDir;
        File file = new File(outputDir);
        if (!file.exists()) {
            file.mkdir();
        }
        String filename = "" + workerName + "_" + runCount;
        File outputFile = new File(outputDir+"/" + filename);
        try {
        	outputFile.getParentFile().mkdirs();
            fos = new FileOutputStream(outputFile);
            pw = new PrintWriter( new OutputStreamWriter( fos));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void registerOperation(long timeToexecute, int txType, int opCount,/*
                                                                               * String
                                                                               * workerId
                                                                               * ,
                                                                               * int
                                                                               * runCount
                                                                               * ,
                                                                               */
            long startTime) {
        pw.println(startTime + "\t" + ((txType == 0) ? "R" : "W") + "\t" + opCount + "\t" + timeToexecute);
    }

/*    public String RawData() {
        StringBuffer string = new StringBuffer();
        string.append("StartTime\tTxType\tOpCount\tTimeToExecute(nano)");
        string.append(workerName+"\n");
        for (int buf = 1; buf <= bufferList.size(); buf++) {
            long[][] ops = bufferList.get(buf - 1);
            int length = (buf != bufferList.size()) ? ops.length : bufferPosition;
            for (int i = 0; i < length; i++)
                string.append(ops[i][3] + "\t" + ((ops[i][1] == 0) ? "R" : "W") + "\t" + ops[i][2] + "\t" + ops[i][0]
                        + "\n");
        }
        return string.toString();
    }
*/
    public int getRunCount() {
        return runCount;
    }

    public String getWorkerName() {
        return workerName;
    }

    public void rawDataToFile() {
        try {
            pw.flush();
            fos.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
