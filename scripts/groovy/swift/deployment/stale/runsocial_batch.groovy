#!/usr/bin/env groovy -classpath .:scripts/groovy:scripts/groovy/lib
package swift.deployment

import static swift.deployment.PlanetLab_3X.*
import static swift.deployment.Tools.*

def __ = onControlC({
    pnuke(AllMachines, "java", 60)
    System.exit(0);
})

workloads=['9':'1','8':'2','7':'3','6':'4','5':'5','4':'6','3':'7','2':'8','1':'9', '0':'10']

['512', '1024', '2048'].each { cache ->
    workloads.each { biasedOps, randomOps ->
        threads = 1
        props = [
            'swift.AsyncCommit':'true',
            'swift.CachePolicy':'CACHED',
            'swift.IsolationLevel':'REPEATABLE_READS',
            'swift.Notifications':'true',
            'swift.CacheSize':cache,
            'swiftsocial.biasedOps':biasedOps,
            'swiftsocial.randomOps':randomOps,
            'swiftsocial.thinkTime':'50',
        ]

        Surrogates = [
            'ec2-176-34-206-15.eu-west-1.compute.amazonaws.com',
            'ec2-50-112-65-41.us-west-2.compute.amazonaws.com',
            'ec2-107-21-171-137.compute-1.amazonaws.com'
        ]

        OUTPREFIX = "workload-cache/"
        new runsocial4batch( getBinding() ).run()
    }
}

//[5, 3, 2, 1].each {
//    threads = it
//    props = [
//        'swift.AsyncCommit':'true',
//        'swift.CachePolicy':'CACHED',
//        'swift.IsolationLevel':'REPEATABLE_READS',
//        'swift.Notifications':'true',
//        'swift.CacheSize':'512',
//        'swiftsocial.biasedOps':'9',
//        'swiftsocial.randomOps':'9',
//        'swiftsocial.thinkTime':'0',
//    ]
//
//    Surrogates = [
//        'ec2-54-228-122-250.eu-west-1.compute.amazonaws.com',
//        'ec2-54-244-156-51.us-west-2.compute.amazonaws.com',
//     ]
//
//    OUTPREFIX = "clt-W99/"
//    new runsocial4batch( getBinding() ).run()
//}
//
//[25, 50, 75, 100, 150].each {
//    threads = 1
//    String thinkTime = it;
//    props = [
//        'swift.AsyncCommit':'true',
//        'swift.CachePolicy':'CACHED',
//        'swift.IsolationLevel':'REPEATABLE_READS',
//        'swift.Notifications':'true',
//        'swift.CacheSize':'512',
//        'swiftsocial.biasedOps':'9',
//        'swiftsocial.randomOps':'9',
//        'swiftsocial.thinkTime': thinkTime,
//    ]
//
//    Surrogates = [
//        'ec2-54-228-122-250.eu-west-1.compute.amazonaws.com',
//        'ec2-54-244-156-51.us-west-2.compute.amazonaws.com',
//     ]
//
//    OUTPREFIX = "clt-W99/"
//    new runsocial4batch( getBinding() ).run()
//}
//
//[3, 2, 1].each {
//    threads = it
//    props = [
//        'swift.AsyncCommit':'true',
//        'swift.CachePolicy':'CACHED',
//        'swift.IsolationLevel':'REPEATABLE_READS',
//        'swift.Notifications':'true',
//        'swift.CacheSize':'512',
//        'swiftsocial.biasedOps':'9',
//        'swiftsocial.randomOps':'9',
//        'swiftsocial.thinkTime':'0',
//    ]
//
//    Surrogates = [
//        'ec2-54-228-122-250.eu-west-1.compute.amazonaws.com',
//     ]
//
//    OUTPREFIX = "clt-W99/"
//    new runsocial4batch( getBinding() ).run()
//}
//
//[25, 50, 75, 100, 150, 200, 250].each {
//    threads = 1
//    String thinkTime = it;
//    props = [
//        'swift.AsyncCommit':'true',
//        'swift.CachePolicy':'CACHED',
//        'swift.IsolationLevel':'REPEATABLE_READS',
//        'swift.Notifications':'true',
//        'swift.CacheSize':'512',
//        'swiftsocial.biasedOps':'9',
//        'swiftsocial.randomOps':'9',
//        'swiftsocial.thinkTime': thinkTime,
//    ]
//
//    Surrogates = [
//        'ec2-54-228-122-250.eu-west-1.compute.amazonaws.com',
//     ]
//
//    OUTPREFIX = "clt-W99/"
//    new runsocial4batch( getBinding() ).run()
//}
//
//new runsocial_batch_dc( getBinding() ).run()
//
////def Sync = ['false', 'true']
////
////def Isolation = [
////    'REPEATABLE_READS',
////]
////
////def Caching = [
////    'CACHED',
////    'STRICTLY_MOST_RECENT'
////]
////
////5.times {
////    Isolation.each {  isolation ->
////        Caching.each { caching ->
////            Sync.each { sync ->
////                threads = 1
////                Surrogates = [
////                   'ec2-54-245-39-94.us-west-2.compute.amazonaws.com',
////                ]
////
////                Scouts = [
////                    'pl2.cs.unm.edu',
////                    'pl3.cs.unm.edu',
////                    'pl4.cs.unm.edu',
////                ]
////                props = [
////                    'swift.AsyncCommit':sync,
////                    'swift.CachePolicy':caching,
////                    'swift.IsolationLevel':isolation,
////                    'swift.Notifications':'true',
////                    'swift.CacheSize':'512',
////                    'swiftsocial.biasedOps':'9',
////                    'swiftsocial.randomOps':'1',
////                    'swiftsocial.thinkTime':'5'
////                ]
////
////                OUTPREFIX = "clt-" + sync + "-" + caching + "-" + isolation + "/"
////                new runsocial4batch( getBinding() ).run()
////            }
////        }
////    }
////}
System.exit(0)