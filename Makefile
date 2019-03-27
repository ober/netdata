.PHONY: netdata

default: netdata

SYSTEM := $(shell uname -s)

ifeq ($(SYSTEM),Darwin)
SSL-BASE :=$(lastword $(wildcard /usr/local/Cellar/openssl/*/))
SED := sed
MYSQL-BASE := $(lastword $(wildcard /usr/local/Cellar/mysql/*/))
LIBYAML-BASE := $(lastword $(wildcard /usr/local/Cellar/libyaml/*/))
#$(eval LDFLAGS := "-L$(SSL-BASE)lib -L$(LIBYAML-BASE)lib -lz -lssl -lyaml -L/usr/local/lib")
#$(eval CPPFLAGS := "-I$(SSL-BASE)include -I$(LIBYAML-BASE)include -I/usr/local/include")
else
LDFLAGS := "-L/usr/lib -lssl -lyaml"
CPPFLAGS := "-I/usr/include"
LIBYAML-BASE := "/usr/include"
SED := sed
endif

#$(info $(CPPFLAGS) is cppflags)
#$(info $(LDFLAGS) is ldflags)

netdata: $(eval CPPFLAGS := "-I$(SSL-BASE)include -I$(LIBYAML-BASE)include -I/usr/local/include")
netdata: $(eval LDFLAGS := "-L$(SSL-BASE)lib -L$(LIBYAML-BASE)lib -lz -lssl -lyaml -L/usr/local/lib")
netdata:
	gxc -O -o netdata -static -exe -g -genv -cc-options $(CPPFLAGS) -ld-options $(LDFLAGS) -gsrc -gsc-flag -keep-c netdata.ss


fast: $(eval CPPFLAGS := "-I$(SSL-BASE)include -I$(LIBYAML-BASE)include -I/usr/local/include")
fast: $(eval LDFLAGS := "-L$(SSL-BASE)lib -L$(LIBYAML-BASE)lib -lz -lssl -lyaml -L/usr/local/lib")
fast:
	gxc -O -o netdata -exe -g -genv -cc-options $(CPPFLAGS) -ld-options $(LDFLAGS) -gsrc -gsc-flag -keep-c netdata.ss
