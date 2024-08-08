To Run The  code follow this stes to install in local system.
Rabbit MQ:
Installations:
Install First:  https://www.erlang.org/downloads
Install  Second: https://www.rabbitmq.com/docs/install-windows

I Dont have a mongo Db Credential for storing the Time stamp data and for retriveing the data.
So i used Postgresql data base to store and get the data.
I used only UTC time if you want to fetch the the data from db need to give the utc in between to date time.

open to two rabbitmq command promment  to run the code one for sending messing and one for to get the data from database.


Post men,To Test the API with End ERL :
GET Method: 
UTC TIME  we need to give to start and end time.
http://192.168.0.101:5000/status_count?start2024-08-08 09:55:25 & end=2024-08-08 2024-08-08 09:58:25
