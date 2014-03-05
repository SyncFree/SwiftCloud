#!/usr/bin/env groovy -classpath .:scripts/groovy:scripts/groovy/lib

import static Tools.*
import static PlanetLab_3X.*

def __ = onControlC({
    pnuke(AllMachines, "java", 60)
    System.exit(0);
})


//    [1].each {
//        threads = it
//        Surrogates = [
//        "ec2-54-246-73-237.eu-west-1.compute.amazonaws.com",
//        "ec2-54-244-138-49.us-west-2.compute.amazonaws.com",
//        "ec2-54-249-212-21.ap-northeast-1.compute.amazonaws.com",
//        "ec2-54-234-212-22.compute-1.amazonaws.com"
//        ]
//        new runsocial( getBinding() ).run()
//    }

def Sync = ['false', 'true']

def Isolation = [
    'REPEATABLE_READS'
]

def Caching = [
    'CACHED',
    'STRICTLY_MOST_RECENT'
]

5.times {
    Isolation.each {  isolation ->
        Caching.each { caching ->
            Sync.each { sync ->
                threads = 1
                
                Surrogates = [
                    'ec2-54-244-189-96.us-west-2.compute.amazonaws.com',
                ]

                EndClients = [
                    'pl2.cs.unm.edu',
                    'pl3.cs.unm.edu',
                    'pl4.cs.unm.edu',
                ]
                
                props = [
                    'swift.AsyncCommit':sync,
                    'swift.CachePolicy':caching,
                    'swift.IsolationLevel':isolation,
                    'swift.Notifications':'true',
                    'swift.CacheSize':'512',
                    'swiftsocial.biasedOps':'9',
                    'swiftsocial.randomOps':'1',
                    'swiftsocial.thinkTime':'5'
                ]
                
                OUTPREFIX = "dc-" + sync + "-" + caching + "-" + isolation + "/"
                new runsocial_dc_scout4batch( getBinding() ).run()
            }
        }
    }
}
System.exit(0)