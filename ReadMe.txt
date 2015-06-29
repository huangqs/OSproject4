compile.sh downloads nodejs from github as well as 
change mode of bin/start_server and bin/stop_server.

To start server n01 just type:
./bin/start_server n01

To kill server n01 just type:
./bin/stop_server n01

To use the client, just visit the site 
http://ip:port/kv/

To test our server, just type:
python3 ./test/paxostest.py

About test:
We use paxostest.py to let 5 server do inserts, 
and we simulate net on\off by allowing\blocking client port.

