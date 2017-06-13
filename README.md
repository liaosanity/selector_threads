# Overview
A Java NIO network server using non-blocking IO all the way through, an overall architecture is:
![image](https://github.com/liaosanity/selector_threads/raw/master/images/overall_architecture.png)
##Each role is introduced as follows:
 *Listener thread, only one, do the accept job via an acceptSelector, then the new connection will be assigned to one of the Reader thread;
 *Reader thread, can be many, do some network protocls check(like blacklist access, etc.) via a readSelector, after reading the data, it will be assigned to one of the Worker thread;
 *Worker thread, can be many, be mainly responsible for business logic, once it got a result, it will be assigned to the Responder thread;
 *Responder thread, only one, be in charge of responding the result to the client via a writeSelector.

# Quick start:
1) mvn compile
2) mvn package
3) bin/start.sh

the file 'src/test/java/com/wolfit/selector_threads/AppTest.java' will show you how to code a client app to communicate with the server.
