# COVIDSCAPE
COVIDSCAPE is the major project for the course EE542: Internet and Cloud Computing. COVIDSCAPE provides a personalized COVID-19 risk prediction service. It is web-hosted and supports a location-based, personalized risk score calculation based on reputable COVID-19 information sources. Additionally, Covidscape leverages the user location data it obtains to further implement contact tracing on diagnosed users and notifies other users of potential contact. 

COVIDSCAPE demonstrates a complete software ecosystem where we take advantage of Amazon Web Services (AWS), Node-Red datacollection, and Machine Learning analytics to finally display on a friendly web-UI. Due to dataset constraints, we currently only provide prediction service in LA area.

Click on the preview below for the video demo on Youtube.

[![YOUTUBE](https://img.youtube.com/vi/F_lyYlsTkUk/0.jpg)](https://www.youtube.com/watch?v=F_lyYlsTkUk)



This is the homepage of COVIDSCAPE. The first thing you see is this Google map and some circles. The circle's size represents COVID concentration in the area. Larger circle means there is a higher density of COVID infected population. The circles are also color coded by COVID growth rate. Red means increasing and Green decreasing. To the top left and top right is the localization functionality. Use it to select the interested location by searching or using current location. Now, below the map, choose the options to best reflect the COVID precautions at that location. It is possible to choose to predict future risk scores, if you don’t want to visit the place right now. The server will return a final prediction score after clicking 'Predict'.

![Home Page](webproject/pics/home1.PNG)

Next is the Report page. Users who are diagnosed with COVID can press this button to notify others who were in close contact with them to be careful.
Click the button, enter your username and it will utilize AWS to send out an SMS and email notification. This also shows COVIDSCAPE UI on mobile devices.

![Report Page](webproject/pics/report1.PNG)

![SMS](webproject/pics/sms.png)

![email](webproject/pics/email.png)

Finally this shows the overall architecture of COVIDSCAPE software system.

![Architecture](webproject/pics/architecture.PNG)


To further learn the details of our implementation, including Machine Learning methodology and Node-Red data collection, please find the full demonstration of COVIDSCAPE on Youtube at the links below: 

https://youtu.be/Fg1HjYlPIEs

https://youtu.be/F_lyYlsTkUk (Backup)

