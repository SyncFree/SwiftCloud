#!/bin/bash
//usr/bin/env groovy -classpath .:scripts/groovy "$0" $@; exit $?

package swift.deployment
import static swift.deployment.Topology.*;

// planetlab test (WARNING: unlikely reproducible performance and issues)
DC_1 = DC([PlanetLab[0]], [PlanetLab[0]])
DC_2 = DC([PlanetLab[2]], [PlanetLab[2]])
DC_3 = DC([PlanetLab[4]], [PlanetLab[4]])

Scouts1 = SGroup( PlanetLab[1..1], DC_1 )
Scouts2 = SGroup( PlanetLab[3..3], DC_2 )
Scouts3 = SGroup( PlanetLab[5..5], DC_3 )
