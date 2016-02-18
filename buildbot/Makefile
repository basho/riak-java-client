
PROJDIR = $(realpath $(CURDIR)/..)

TOOLS_DIR = $(PROJDIR)/tools/devrel
RIAK_CONF = $(RIAK_DIR)/etc/riak.conf
ADV_CONF = $(RIAK_DIR)/etc/advanced.config
RIAK_ADMIN = $(RIAK_DIR)/bin/riak-admin

preconfigure:
	$(TOOLS_DIR)/gen-riak-conf $(RIAK_CONF) 8098 8087
	$(TOOLS_DIR)/strong-consistency-conf $(RIAK_CONF)
	$(TOOLS_DIR)/gen-adv-conf $(ADV_CONF)

configure:
	$(TOOLS_DIR)/riak-cluster-config $(RIAK_ADMIN) 8098 true false

compile:
	@cd ..; mvn clean compile

lint:
	@echo "No lint task"

test: test-normal

test-normal:
	@cd ..; mvn -Pitest,default -DargLine="-Dcom.basho.riak.2i=true -Dcom.basho.riak.yokozuna=true -Dcom.basho.riak.buckettype=true -Dcom.basho.riak.crdt=true" verify

test-security:
	${RIAK_ADMIN} security add-user tester password=tester
	${RIAK_ADMIN} security add-source tester 127.0.0.1/32 password
	${RIAK_ADMIN} security grant riak_kv.get,riak_kv.put,riak_kv.delete,riak_kv.index,riak_kv.list_keys,riak_kv.list_buckets,riak_core.get_bucket,riak_core.set_bucket,riak_core.get_bucket_type,riak_core.set_bucket_type,search.admin,search.query,riak_kv.mapreduce on any to tester
	${RIAK_ADMIN} security enable
	@cd ..; mvn -Pitest -DargLine="-Dcom.basho.riak.security=true" verify
