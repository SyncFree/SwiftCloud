package swift.stats

import umontreal.iro.lecuyer.stat.Tally

class MetadataLogsProcessor {
    static CATEGORY_FULL_SIZE = "full message size"
    static CATEGORY_GLOBAL_METADATA = "global metadata"
    static CATEGORY_OBJECT_METADATA = "object metadata"
    static CATEGORY_GLOBAL_METADATA_PRECISE = "exact global metadata"
    static CATEGORY_VECTOR_SIZE = "vector size"
    static CATEGORY_HOLES_NUMBER = "holes number"
    static CATEGORY_BATCH_SIZE = "batch size"
    static NOISE_TIME_CUT_BEFORE_S = 30
    static NOISE_TIME_CUT_AFTER_S = 200

    // Call with a file and empty or prefilled maps
    static processFile(File f, Map categoriesMessagesSessionsSeriesMap, Map categoriesMessagesTallyMap ) {
        println "Processing file " + f

        // Create nested maps and objects on demand
        if (categoriesMessagesSessionsSeriesMap != null) {
            categoriesMessagesSessionsSeriesMap = categoriesMessagesSessionsSeriesMap.withDefault { category ->
                [:].withDefault{ message ->
                    [:].withDefault { session -> []}
                }
            }
        }

        if (categoriesMessagesTallyMap != null) {
            categoriesMessagesTallyMap = categoriesMessagesTallyMap.withDefault { category ->
                [:].withDefault{ message ->
                    new Tally(message)
                }
            }
        }

        long T0 = -1
        f.eachLine { String l ->
            String[] fields = l.split(",")
            if( ! l.startsWith(";") && !l.startsWith("SYS") && l.contains("METADATA_") && fields.length >= 10) {
                try {
                    String sessionId = fields[0]
                    long T = Long.valueOf(fields[1])
                    String message = fields[2].substring("METADATA_".size())
                    int messageSize = Integer.valueOf(fields[3])
                    int objectMetadataData = Integer.valueOf(fields[4])
                    def globalMetadata = messageSize - objectMetadataData
                    int dataOnly = Integer.valueOf(fields[5])
                    def objectMetadata = objectMetadataData- dataOnly
                    int globalMetadataExplicit = Integer.valueOf(fields[6])
                    int batchSize = Integer.valueOf(fields[7])
                    int vvSize = Integer.valueOf(fields[8])
                    int vvHolesNumber = Integer.valueOf(fields[9])

                    if (T0 < 0) {
                        T0 = T
                    }
                    if ((T-T0)/1000.0 < NOISE_TIME_CUT_BEFORE_S || (T-T0)/1000.0 > NOISE_TIME_CUT_AFTER_S) {
                        return
                    }

                    if (categoriesMessagesSessionsSeriesMap != null) {
                        categoriesMessagesSessionsSeriesMap[CATEGORY_FULL_SIZE][message][sessionId] << String.format("%.3f %d", (T - T0)/1000.0, messageSize)
                        categoriesMessagesSessionsSeriesMap[CATEGORY_GLOBAL_METADATA][message][sessionId] << String.format("%.3f %d", (T - T0)/1000.0, globalMetadata)
                        categoriesMessagesSessionsSeriesMap[CATEGORY_OBJECT_METADATA][message][sessionId] << String.format("%.3f %d", (T - T0)/1000.0, objectMetadata)
                        categoriesMessagesSessionsSeriesMap[CATEGORY_GLOBAL_METADATA_PRECISE][message][sessionId] << String.format("%.3f %d", (T - T0)/1000.0, globalMetadataExplicit)
                        categoriesMessagesSessionsSeriesMap[CATEGORY_BATCH_SIZE][message][sessionId] << String.format("%.3f %d", (T - T0)/1000.0, batchSize)
                        categoriesMessagesSessionsSeriesMap[CATEGORY_VECTOR_SIZE][message][sessionId] << String.format("%.3f %d", (T - T0)/1000.0, vvSize)
                        categoriesMessagesSessionsSeriesMap[CATEGORY_HOLES_NUMBER][message][sessionId] << String.format("%.3f %d", (T - T0)/1000.0, vvHolesNumber)
                    }
                    if (categoriesMessagesTallyMap != null) {
                        categoriesMessagesTallyMap[CATEGORY_FULL_SIZE][message].add((double) messageSize)
                        categoriesMessagesTallyMap[CATEGORY_GLOBAL_METADATA][message].add((double) globalMetadata)
                        categoriesMessagesTallyMap[CATEGORY_OBJECT_METADATA][message].add((double) objectMetadata)
                        categoriesMessagesTallyMap[CATEGORY_GLOBAL_METADATA_PRECISE][message].add((double) globalMetadataExplicit)
                        categoriesMessagesTallyMap[CATEGORY_BATCH_SIZE][message].add((double) batchSize)
                        categoriesMessagesTallyMap[CATEGORY_VECTOR_SIZE][message].add((double) vvSize)
                        categoriesMessagesTallyMap[CATEGORY_HOLES_NUMBER][message].add((double) vvHolesNumber)
                    }
                } catch (NumberFormatException x) {
                    System.err.println("Ingoring ununderstandable line: " + l)
                }
            }
        }
    }
}
