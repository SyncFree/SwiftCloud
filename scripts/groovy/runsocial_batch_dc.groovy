#!/usr/bin/env groovy -classpath .:scripts/groovy:scripts/groovy/lib

import static Tools.*
import static PlanetLab_3X.*

def __ = onControlC({
    pnuke(AllMachines, "java", 60)
    System.exit(0);
})

[3, 2, 1].each {
    threads = it
    props = [
        'swift.AsyncCommit':'true',
        'swift.CachePolicy':'CACHED',
        'swift.IsolationLevel':'REPEATABLE_READS',
        'swift.Notifications':'true',
        'swift.CacheSize':'0',
        'swiftsocial.biasedOps':'9',
        'swiftsocial.randomOps':'9',
        'swiftsocial.thinkTime':'0',
    ]

    Surrogates = [
        'ec2-54-228-122-250.eu-west-1.compute.amazonaws.com',
        'ec2-54-244-156-51.us-west-2.compute.amazonaws.com',
        'ec2-23-22-68-130.compute-1.amazonaws.com'
    ]


    OUTPREFIX = "dc-W99/"
    new runsocial_dc_scout4batch( getBinding() ).run()
}

[100, 200, 500, 1000, 2000].each {
    threads = 1
    String thinkTime = it;
    props = [
        'swift.AsyncCommit':'true',
        'swift.CachePolicy':'CACHED',
        'swift.IsolationLevel':'REPEATABLE_READS',
        'swift.Notifications':'true',
        'swift.CacheSize':'0',
        'swiftsocial.biasedOps':'9',
        'swiftsocial.randomOps':'9',
        'swiftsocial.thinkTime': thinkTime,
    ]

    Surrogates = [
        'ec2-54-228-122-250.eu-west-1.compute.amazonaws.com',
        'ec2-54-244-156-51.us-west-2.compute.amazonaws.com',
        'ec2-23-22-68-130.compute-1.amazonaws.com'
    ]


    OUTPREFIX = "dc-W99/"
    new runsocial_dc_scout4batch( getBinding() ).run()
}

[3, 2, 1].each {
    threads = it
    props = [
        'swift.AsyncCommit':'true',
        'swift.CachePolicy':'CACHED',
        'swift.IsolationLevel':'REPEATABLE_READS',
        'swift.Notifications':'true',
        'swift.CacheSize':'0',
        'swiftsocial.biasedOps':'9',
        'swiftsocial.randomOps':'9',
        'swiftsocial.thinkTime':'0',
    ]

    Surrogates = [
        'ec2-54-228-122-250.eu-west-1.compute.amazonaws.com',
        'ec2-54-244-156-51.us-west-2.compute.amazonaws.com',
    ]


    OUTPREFIX = "dc-W99/"
    new runsocial_dc_scout4batch( getBinding() ).run()
}

[100, 200, 500, 1000, 2000].each {
    threads = 1
    String thinkTime = it;
    props = [
        'swift.AsyncCommit':'true',
        'swift.CachePolicy':'CACHED',
        'swift.IsolationLevel':'REPEATABLE_READS',
        'swift.Notifications':'true',
        'swift.CacheSize':'0',
        'swiftsocial.biasedOps':'9',
        'swiftsocial.randomOps':'9',
        'swiftsocial.thinkTime': thinkTime,
    ]

    Surrogates = [
        'ec2-54-228-122-250.eu-west-1.compute.amazonaws.com',
        'ec2-54-244-156-51.us-west-2.compute.amazonaws.com',
    ]


    OUTPREFIX = "dc-W99/"
    new runsocial_dc_scout4batch( getBinding() ).run()
}

[3, 2, 1].each {
    threads = it
    props = [
        'swift.AsyncCommit':'true',
        'swift.CachePolicy':'CACHED',
        'swift.IsolationLevel':'REPEATABLE_READS',
        'swift.Notifications':'true',
        'swift.CacheSize':'0',
        'swiftsocial.biasedOps':'9',
        'swiftsocial.randomOps':'9',
        'swiftsocial.thinkTime':'0',
    ]

    Surrogates = [
        'ec2-54-228-122-250.eu-west-1.compute.amazonaws.com',
    ]


    OUTPREFIX = "dc-W99/"
    new runsocial_dc_scout4batch( getBinding() ).run()
}

[100, 200, 500, 1000, 2000].each {
    threads = 1
    String thinkTime = it;
    props = [
        'swift.AsyncCommit':'true',
        'swift.CachePolicy':'CACHED',
        'swift.IsolationLevel':'REPEATABLE_READS',
        'swift.Notifications':'true',
        'swift.CacheSize':'0',
        'swiftsocial.biasedOps':'9',
        'swiftsocial.randomOps':'9',
        'swiftsocial.thinkTime': thinkTime,
    ]

    Surrogates = [
        'ec2-54-228-122-250.eu-west-1.compute.amazonaws.com',
    ]


    OUTPREFIX = "dc-W99/"
    new runsocial_dc_scout4batch( getBinding() ).run()
}

////    [1].each {
////        threads = it
////        Surrogates = [
////        "ec2-54-246-73-237.eu-west-1.compute.amazonaws.com",
////        "ec2-54-244-138-49.us-west-2.compute.amazonaws.com",
////        "ec2-54-249-212-21.ap-northeast-1.compute.amazonaws.com",
////        "ec2-54-234-212-22.compute-1.amazonaws.com"
////        ]
////        new runsocial( getBinding() ).run()
////    }
//
//def Sync = ['false', 'true']
//
//def Isolation = [
//    'REPEATABLE_READS'
//]
//
//def Caching = [
//    'CACHED',
//    'STRICTLY_MOST_RECENT'
//]
//
//5.times {
//    Isolation.each {  isolation ->
//        Caching.each { caching ->
//            Sync.each { sync ->
//                threads = 1
//
//                Surrogates = [
//                     'ec2-54-245-39-94.us-west-2.compute.amazonaws.com',
//                ]
//
//                EndClients = [
//                    'pl2.cs.unm.edu',
//                    'pl3.cs.unm.edu',
//                    'pl4.cs.unm.edu',
//                ]
//
//                props = [
//                    'swift.AsyncCommit':sync,
//                    'swift.CachePolicy':caching,
//                    'swift.IsolationLevel':isolation,
//                    'swift.Notifications':'true',
//                    'swift.CacheSize':'512',
//                    'swiftsocial.biasedOps':'9',
//                    'swiftsocial.randomOps':'1',
//                    'swiftsocial.thinkTime':'5'
//                ]
//
//                OUTPREFIX = "dc-" + sync + "-" + caching + "-" + isolation + "/"
//                new runsocial_dc_scout4batch( getBinding() ).run()
//            }
//        }
//    }
//}
System.exit(0)