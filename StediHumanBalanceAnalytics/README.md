# STEDI Human Balance Analytics

This is a Udacity Project as part of the Data Engineering with AWS nanodegree. The following section is the description from Udacity

### Problem Description
In this project, you'll act as a data engineer for the STEDI team to build a data lakehouse solution for sensor data that trains a machine learning model.

The STEDI Team has been hard at work developing a hardware STEDI Step Trainer that:
* trains the user to do a STEDI balance exercise;
* and has sensors on the device that collect data to train a machine-learning algorithm to detect steps;
* has a companion mobile app that collects customer data and interacts with the device sensors.
STEDI has heard from millions of early adopters who are willing to purchase the STEDI Step Trainers and use them.

Several customers have already received their Step Trainers, installed the mobile application, and begun using them together to test their balance. The Step Trainer is just a motion sensor that records the distance of the object detected. The app uses a mobile phone accelerometer to detect motion in the X, Y, and Z directions.

The STEDI team wants to use the motion sensor data to train a machine learning model to detect steps accurately in real-time. Privacy will be a primary consideration in deciding what data can be used.

Some of the early adopters have agreed to share their data for research purposes. **Only these customers’ Step Trainer and accelerometer data should be used in the training data for the machine learning model.**

As a data engineer on the STEDI Step Trainer team, you'll need to extract the data produced by the STEDI Step Trainer sensors and the mobile app, and curate them into a data lakehouse solution on AWS so that Data Scientists can train the learning model.

### Project Structure
The project is organized into the following folders:
* `Screenshots` contains screenshots of select queries against the data in the landing, trusted, and curated zones
  * snapshots are taken with Athena
  * data is also dumped into S3 buckets at each step
* `DDLs` contains the `.sql` DDL files for tables in the landing zone
* `GlueJobs` contains the data pipelines to create tables in the trusted and curated zones
  * each target node writes to a glue table and also to S3
