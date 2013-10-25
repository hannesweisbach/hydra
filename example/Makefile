CXXFLAGS   += -std=c++11 -ggdb -O3
LIBS		= -libverbs -lrdmacm

TARGETS = dht_server dht_client julian

all :
	for i in $(TARGETS); do $(MAKE) $$i; done

dht_server : dht_server.cc
	$(CXX) $(CXXFLAGS) $(LIBS) -o $@ $<

dht_client : dht_client.cc
	$(CXX) $(CXXFLAGS) $(LIBS) -o $@ $<

julian : julian.cc
	$(CXX) $(CXXFLAGS) $(LIBS) -o $@ $<

r run :
	echo "if [ -e ~/.bashrc ]; then . ~/.bashrc; fi; echo "running \$$BASH_CMD ..."; \$$BASH_CMD" > bash_cmd
	BASH_CMD="./dht_server 8000" urxvt -e bash --init-file bash_cmd &
	sleep .2
	BASH_CMD="./dht_client 127.0.0.1 8000" urxvt -e bash --init-file bash_cmd &

cl clean :
	rm -fr bash_cmd *.o $(TARGETS)
