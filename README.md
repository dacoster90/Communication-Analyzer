# Communication Analyzer

### Description
The Communication Analyzer is a console application designed to analyze and provide insights about messages exchanged between Onboard Computers (or OBC) of locomotives and the Safety Train Control system (or STC) of production railway operations based in Gabon. The STC is operated by dispatchers and is responsible for coordenating railway traffic, such as providing the trains with proceed authorities, adding/removing speed restrictions, allow maintenance on the tracks and so on. 
The STC sends messages to the OBCs, so the equipment in the locomotives can behave accordingly. Also the OBC sends out information (such as location, or requests) back to the Control Centre. 

A common issue with this architecture is the fact that message can suffer delays in their delivery, or even are not being delivered. This problem directly impacts the operations of the railroad. Before the development of this application, neither the software developer nor the client had really an idea of how many messages were delivered within an acceptable timeframe.There was no real way to actually measure this, besides looking at the number of tickets opened by the client (not really a good indicator though). That's when I got the idea to develop the Communication Analyzer. This tool provides information and insights, such as total messages sent, messages with delay, average message time, delays per channel, delays per locomotive, delays per block section and so on. It also generates different artificats, such as a dashboard (in PowerBI), a extensive Communication Report (in Word), logfiles and graphs (time-series, histograms, dispersion graphs etc.).

### Languages and Technologies
- Python
- SQL
- Power BI
- Oracle
- Jupyter Notebook

### Compatibility
The Communication Analyzer is compatible with STC 1.7.x database and logfiles for Setrag.
The Communication Analyzer is compatible with the ABR 1.x logfiles for Setrag. 

### Pre-requisites
1. Install Python
2. Install Oracle
3. Import STC_SETRAG dumpfile
4. Download the Message Manager logfiles
5. Download and merge the Message Manager logfiles to one unique file in CMD by executing the command: copy *MessageManager.log MessageManager.log
5. Download and merge the ABR logfiles to one unique file in CMD by executing the command: copy app.log* ABR.log
6. Install PowerBI

### Installation
1. Run Package_Installer.bat to install Python modules.
2. Run the following script: 1. Create Owner DATA_ANALYIS.sql.
3. Run the following script on schema DATA_ANALYSIS: 2. Create Database DATA_ANALYSIS.sql.
4. Run the following script on schema DATA_ANALYSIS: 3. Create IMAGES directory.sql (on the script, insert the directory where the images will be stored).
5. Open the config.ini file and fill out all parameters.

### How to run
1. Open CMD and navigate to the Communication Analyzer directory.
2. Execute 'python Messages_Analysis.py'
3. Insert month and year
4. Whenever the application generates a chart image, please close it.

### Images
![dashboard](https://i.ibb.co/1Jbby1t/dashboard.png)
![application](https://i.ibb.co/N9wGbnL/application.png)
![graph](https://i.ibb.co/yYKtbPC/graphs.png)
![report](https://i.ibb.co/rx7NY5L/report.png)
