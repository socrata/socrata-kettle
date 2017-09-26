Socrata Plugin for Pentaho Kettle
================

Last updated: February 11, 2016

Authors: [Malinda Curtis](http://www.github.com/malindac)

Looking for the latest release? Get it here: https://github.com/socrata/socrata-kettle/releases

## General Information
[Pentaho Kettle](http://community.pentaho.com/projects/data-integration/) is an open source Extraction, Transformation, and Loading (ETL) tool.  The Socrata Output plugin allows for transformation workflows created in Kettle to be published to a Socrata dataset.

### Compatibility
This version of the Socrata Plugin for Pentaho Kettle is compatible with Pentaho Kettle version 5.4 or later.

### Installation
Unzip and place the SocrataPlugin folder into the plugins/steps directory of your Pentaho Kettle installation.  Relaunch Kettle and you should see the Socrata Output plugin under Output for use in your transformations.

*Note:* Mac versions prior to El Capitan require that Kettle be launched using the spoon.command file.  Launching Kettle using the .app file will result in a Java version error.

*Note:* Kettle version 6.0 does not ship with the steps directory.  For installation to succeed, a steps directory will need to be created inside the plugins directory.
