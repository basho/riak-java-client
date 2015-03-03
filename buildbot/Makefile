RIAK_CONF = ${RIAK_DIR}/etc/riak.conf
ADVANCED_CONF = ${RIAK_DIR}/etc/advanced.config
# RIAK = ${RIAK_DIR}/bin/riak
RIAK_ADMIN = ${RIAK_DIR}/bin/riak-admin
CERTS_DIR = $(shell pwd)/../src/test/resources

preconfigure:
	echo "storage_backend = leveldb" >> ${RIAK_CONF}
	echo "search = on" >> ${RIAK_CONF}
	echo "listener.protobuf.internal = 127.0.0.1:8087" >> ${RIAK_CONF}
	echo "ssl.keyfile = ${CERTS_DIR}/key.pem" >> ${RIAK_CONF}
	echo "ssl.certfile = ${CERTS_DIR}/cert.pem" >> ${RIAK_CONF}
	echo "ssl.cacertfile = ${CERTS_DIR}/cacert.pem" >> ${RIAK_CONF}

configure:
	${RIAK_ADMIN} bucket-type create jvtest_yz_search '{"props":{}}'
	${RIAK_ADMIN} bucket-type create jvtest_test_type '{"props":{}}'
	${RIAK_ADMIN} bucket-type create jvtest_counters '{"props":{"datatype":"counter"}}'
	${RIAK_ADMIN} bucket-type create jvtest_sets '{"props":{"datatype":"set"}}'
	${RIAK_ADMIN} bucket-type create jvtest_maps '{"props":{"datatype":"map"}}'
	${RIAK_ADMIN} bucket-type activate jvtest_yz_search
	${RIAK_ADMIN} bucket-type activate jvtest_test_type
	${RIAK_ADMIN} bucket-type activate jvtest_counters
	${RIAK_ADMIN} bucket-type activate jvtest_sets
	${RIAK_ADMIN} bucket-type activate jvtest_maps

compile:
	@cd ..; mvn clean compile

lint:
	@echo "No lint task"

test: test-normal test-security

test-normal:
	@cd ..; mvn -Pitest,default -DargLine="-Dcom.basho.riak.2i=true -Dcom.basho.riak.yokozuna=true -Dcom.basho.riak.buckettype=true -Dcom.basho.riak.crdt=true" verify

test-security:
	${RIAK_ADMIN} security add-user tester password=tester
	${RIAK_ADMIN} security add-source tester 127.0.0.1/32 password
	${RIAK_ADMIN} security grant riak_kv.get,riak_kv.put,riak_kv.delete,riak_kv.index,riak_kv.list_keys,riak_kv.list_buckets,riak_core.get_bucket,riak_core.set_bucket,riak_core.get_bucket_type,riak_core.set_bucket_type,search.admin,search.query,riak_kv.mapreduce on any to tester
	${RIAK_ADMIN} security enable
	@cd ..; mvn -Pitest -DargLine="-Dcom.basho.riak.security=true" verify
