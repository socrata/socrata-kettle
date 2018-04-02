Socrata Plugin for Pentaho Data Integration
================

Last updated: October 26, 2017

Authors: [Malinda Curtis](http://www.github.com/malindac)

Looking for the latest release? Get it here: https://github.com/socrata/socrata-kettle/releases

## General Information
[Pentaho Data Integration (PDI)](https://community.hds.com/docs/DOC-1009855) is an open source Extraction, Transformation, and Loading (ETL) tool.  The Socrata Output plugin allows for transformation workflows created in PDI to be published to a Socrata dataset.

### Compatibility
This version of the Socrata Plugin for Pentaho Data Integration is compatible with Pentaho Data Integration version 5.4 or later.

### Installation
Download and place the socrata.jar file into the plugins/steps directory of your Pentaho Data Integration installation.  Relaunch PDI and you should see the Socrata Output plugin under Output for use in your transformations.

*Note:* PDI versions 6.0 and later do not ship with the steps directory.  For installation to succeed, a steps directory will need to be created inside the plugins directory.

*Note:* Mac versions prior to El Capitan require that PDI be launched using the spoon.command file.  Launching PDI using the .app file will result in a Java version error.