#!/usr/bin/env groovy -classpath .:scripts/groovy:scripts/groovy/lib:scripts/groovy/stats:scripts/groovy/stats/lib::scripts/groovy/stats/lib/ssj.jar

import static Tools.*
import static PlanetLab_3X.*


DIR = new File("/Users/smd/Dropbox/bitbucket-git/swiftcloud-gforce/results/swiftsocial/SOSP/clt_cdfs")
DC = new File("/Users/smd/Dropbox/bitbucket-git/swiftcloud-gforce/results/swiftsocial/SOSP/dc_cdfs")

CACHEPOLICY = 'CACHED'
ISOLATION = 'REPEATABLE_READS'
new runsocial_latency_cdfs( getBinding() ).run()
