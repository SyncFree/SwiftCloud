package swift.deployment

import java.util.concurrent.atomic.AtomicInteger


class SwiftDoc extends SwiftBase {
    static String SCOUT_CMD = "-Xincgc -cp swiftcloud.jar -Xincgc -Djava.util.logging.config.file=all_logging.properties swift.application.swiftdoc.SwiftDocBenchmark"

    static String CS_SCOUT_CMD = "-Xincgc -cp swiftcloud.jar -Xincgc -Djava.util.logging.config.file=all_logging.properties swift.application.social.cs.SwiftSocialBenchmarkServer"
    static String CS_ENDCLIENT_CMD = "-Xincgc -cp swiftcloud.jar -Djava.util.logging.config.file=all_logging.properties swift.application.social.cs.SwiftSocialBenchmarkClient"

    static void runStandaloneScouts( List scouts, String server, String iterations, String isolationLevel, String cachepolicy, String notifications, String heap ="512m" ) {
        def cmd = { host ->
            String clientId = 1 + scouts.indexOf( host )
            def res = "nohup java -Xmx" + heap + " " + SCOUT_CMD + " " + server + " " +  iterations + "  " + clientId + "  " + isolationLevel + " " + cachepolicy + " " + notifications
            res += "> scout-stdout.txt 2> scout-stderr.txt < /dev/null &"
            return res;
        }

        AtomicInteger n = new AtomicInteger();
        def resHandler = { host, res ->
            def str = n.incrementAndGet() + "/" + scouts.size() + (res < 1 ? " [ OK ]" : " [FAILED]") + " : " + host
            println str
        }
        Parallel.rsh( scouts, cmd, resHandler, true, 500000)
    }
}