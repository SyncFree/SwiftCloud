#!/usr/bin/env groovy -classpath .:scripts/groovy:scripts/groovy/lib

import static Tools.*
import static PlanetLab_3X.*


DIR = new File("/Users/smd/Dropbox/bitbucket-git/swiftcloud-gforce/results/swiftsocial/SOSP/clt_cdfs")
ISOLATION = 'SNAPSHOT_ISOLATION'
new runsocial_latency_cdf( getBinding() ).run()
