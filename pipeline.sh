#!/bin/bash

###########################
# GENERATION AND EMISSIONS
###########################
# scrape and aggregate fossil data from EPA
python aggregator.py 2010
python aggregator.py 2011
python aggregator.py 2012


##############
# INTERCHANGE
##############

# download ISONE data by hand
# see data/isos/isone/README

# NYISO
python clients/nyiso_client.py 2010,2011,2012

# MISO
python clients/miso_client.py 2010,2011
