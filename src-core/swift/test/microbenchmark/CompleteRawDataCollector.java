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

public class CompleteRawDataCollector {

    // [0] -> timeToexecute, [1] -> txType, [2] -> opCount, [4]
    // -> startTime
    long[][] operationInfo;
    int bufferPosition, runCount;
    List<long[][]> bufferList;
    private String workerName;
    private String outputDir;

    public CompleteRawDataCollector(int initialSize, String workerName, int runCount, String outputDir) {
        bufferList = new ArrayList<long[][]>();
        operationInfo = new long[initialSize][4];
        bufferList.add(operationInfo);
        bufferPosition = 0;
        this.workerName = workerName;
        this.runCount = runCount;
        this.outputDir = outputDir;

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

        if (bufferPosition == operationInfo.length) {
            operationInfo = new long[operationInfo.length][4];
            bufferList.add(operationInfo);
            bufferPosition = 0;
            System.out.println("NEW BUFFER CREATED");
        }

        else {
            operationInfo[bufferPosition][0] = timeToexecute;
            operationInfo[bufferPosition][1] = txType;
            operationInfo[bufferPosition][2] = opCount;
            // operationInfo[bufferPosition][3] =
            // workerId.charAt(workerId.length() - 1) + (runCount * 10);
            operationInfo[bufferPosition][3] = startTime;
            bufferPosition++;
        }
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
        File file = new File(outputDir);
        if (!file.exists()) {
            file.mkdir();
        }
        String filename = "" + workerName + "_" + runCount;
        File outputFile = new File(outputDir+"/" + filename);
        FileOutputStream fos;
        try {
            fos = new FileOutputStream(outputFile);
            PrintWriter pw = new PrintWriter( new OutputStreamWriter( fos));
            pw.println("StartTime\tTxType\tOpCount\tTimeToExecute(nano)");
            pw.println(workerName+"\n");
            for (int buf = 1; buf <= bufferList.size(); buf++) {
                long[][] ops = bufferList.get(buf - 1);
                int length = (buf != bufferList.size()) ? ops.length : bufferPosition;
                for (int i = 0; i < length; i++)
                    pw.println(ops[i][3] + "\t" + ((ops[i][1] == 0) ? "R" : "W") + "\t" + ops[i][2] + "\t" + ops[i][0]
                            + "\n");
            }
            fos.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
