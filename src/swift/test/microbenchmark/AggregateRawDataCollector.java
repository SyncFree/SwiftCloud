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

public class AggregateRawDataCollector {
    public static int MAX_TYPES = 20;
    public static int MAX_OPCOUNT = 10;

    // [0] -> timeToexecute, [1] -> txType, [2] -> opCount, [4]
    // -> startTime
    long minVals1[][];   // [i][j] -> i: txType; j-> opCount; 
    long minVals2[];   // [i][j] -> i: txType; 
    long maxVals1[][];   // [i][j] -> i: txType; j-> opCount; 
    long maxVals2[];   // [i][j] -> i: txType; 
    long sumVals1[][];   // [i][j] -> i: txType; j-> opCount; 
    long sumVals2[];   // [i][j] -> i: txType; 
    long countVals1[][];   // [i][j] -> i: txType; j-> opCount; 
    long countVals2[];   // [i][j] -> i: txType; 
    int runCount;
    int maxType;
    int maxOpCount;
    private String workerName;
    private String outputDir;

    public AggregateRawDataCollector(int initialSize, String workerName, int runCount, String outputDir) {
        this.workerName = workerName;
        this.runCount = runCount;
        this.outputDir = outputDir;
        init();
    }
    
    void init() {
        minVals1 = new long[MAX_TYPES][MAX_OPCOUNT];
        minVals2 = new long[MAX_TYPES];
        maxVals1 = new long[MAX_TYPES][MAX_OPCOUNT];
        for( int i = 0; i < MAX_TYPES; i++)
            for( int j = 0; j < MAX_OPCOUNT; j++)
                maxVals1[i][j] = Long.MAX_VALUE;
        maxVals2 = new long[MAX_TYPES];
        for( int i = 0; i < maxVals2.length; i++)
            maxVals2[i] = Long.MAX_VALUE;
        sumVals1 = new long[MAX_TYPES][MAX_OPCOUNT];
        sumVals2 = new long[MAX_TYPES];
        countVals1 = new long[MAX_TYPES][MAX_OPCOUNT];
        countVals2 = new long[MAX_TYPES];
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
        if( txType > maxType)
            maxType = txType;
        if( opCount > maxOpCount)
            maxOpCount = opCount;
        sumVals1[txType][opCount] += timeToexecute;
        sumVals2[txType] += timeToexecute;
        countVals1[txType][opCount]++;
        countVals2[txType]++;
        if( timeToexecute > maxVals1[txType][opCount])
            maxVals1[txType][opCount] = timeToexecute;
        if( timeToexecute > maxVals2[txType])
            maxVals2[txType] = timeToexecute;
        if( timeToexecute < minVals1[txType][opCount])
            minVals1[txType][opCount] = timeToexecute;
        if( timeToexecute < minVals2[txType])
            minVals2[txType] = timeToexecute;
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
            pw.println("StartTime\tTxType\tOpCount\tTimeToExecute(nano)\tAvgTimeToExecute(nano)\tMaxTimeToExecute(nano)\tMinTimeToExecute(nano)\tCount");
            pw.println(workerName+"\n");
            for( int i= 0; i < maxType; i++)
                for( int j = 0; j < maxOpCount; j++) {
                    pw.println( "0\t" + i + "\t" + j + "\t" + 
                            sumVals1[i][j] + "\t" + 
                            (countVals1[i][j] == 0 ? "0" : (sumVals1[i][j]/countVals1[i][j])) + "\t" + 
                            maxVals1[i][j] + "\t" + 
                            minVals1[i][j] + "\t" + 
                            countVals1[i][j] );
                }
            fos.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
