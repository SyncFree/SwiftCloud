package swift.stats

import umontreal.iro.lecuyer.stat.Tally

class MetadataLogsProcessor {
    static CATEGORY_FULL_SIZE = "full message size"
    static CATEGORY_GLOBAL_METADATA = "global metadata"
    static CATEGORY_OBJECT_METADATA = "object metadata"
    static CATEGORY_HOLES_NUMBER = "holes number"

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
            if( ! l.startsWith(";") && !l.startsWith("SYS") && l.contains("METADATA_") && fields.length == 7) {
                String sessionId = fields[0]
                long T = Long.valueOf(fields[1])
                String message = fields[2].substring("METADATA_".size())
                int messageSize = Integer.valueOf(fields[3])
                int objectMetadataData = Integer.valueOf(fields[4])
                def globalMetadata = messageSize - objectMetadataData
                int dataOnly = Integer.valueOf(fields[5])
                def objectMetadata = objectMetadataData- dataOnly
                int vvHolesNumber = Integer.valueOf(fields[6])
                if (T0 < 0) {
                    T0 = T
                }

                if (categoriesMessagesSessionsSeriesMap != null) {
                    categoriesMessagesSessionsSeriesMap[CATEGORY_FULL_SIZE][message][sessionId] << String.format("%.3f %d", (T - T0)/1000.0, messageSize)
                    categoriesMessagesSessionsSeriesMap[CATEGORY_GLOBAL_METADATA][message][sessionId] << String.format("%.3f %d", (T - T0)/1000.0, globalMetadata)
                    categoriesMessagesSessionsSeriesMap[CATEGORY_OBJECT_METADATA][message][sessionId] << String.format("%.3f %d", (T - T0)/1000.0, objectMetadata)
                    categoriesMessagesSessionsSeriesMap[CATEGORY_HOLES_NUMBER][message][sessionId] << String.format("%.3f %d", (T - T0)/1000.0, vvHolesNumber)
                }
                if (categoriesMessagesTallyMap != null) {
                    categoriesMessagesTallyMap[CATEGORY_FULL_SIZE][message].add((double) messageSize)
                    categoriesMessagesTallyMap[CATEGORY_GLOBAL_METADATA][message].add((double) globalMetadata)
                    categoriesMessagesTallyMap[CATEGORY_OBJECT_METADATA][message].add((double) objectMetadata)
                }
            }
        }
    }
}
