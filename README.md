# cardatabase
once file is written locally, pls commit changes here so all members are able to refer to latest sql file thx

--Before Execute SQL Script PLS RUN THIS--
CREATE USER AdvDB PROFILE "DEFAULT" IDENTIFIED BY abcd

 GRANT CONNECT TO AdvDB;

 GRANT GRANT ANY PRIVILEGES TO AdvDB WITH ADMIN OPTION; 	

 grant create session, create table, unlimited tablespace to AdvDB;


 CONNECT AdvDB /abcd;

 --Then create the user AdvDB--