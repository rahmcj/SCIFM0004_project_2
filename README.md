# SCIFM0004_project_2
This is an application that has amended code from the HZZ analysis run by the ATLAS experiment at the Large Hadron Collider so that it can be run in multiple containers.

The original HZZ analysis code can be found here: https://github.com/atlas-outreach-data-tools/notebooks-collection-opendata/blob/master/13-TeV-examples/uproot_python/HZZAnalysis.ipynb

To run the code you will need to write the following on the command line:
docker-compose up 

If you would like to change the number of workers being used, then amend the "replicas" field in the docker-compose.yml file, it is currently set at 2.

