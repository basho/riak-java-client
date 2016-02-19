
PROJDIR = $(realpath $(CURDIR)/..)
RESOURCES_DIR = $(PROJDIR)/src/test/resources/
TOOLS_DIR = $(PROJDIR)/tools/devrel
CERTS_DIR = $(PROJDIR)/tools/test-ca/certs
PRIVATE_DIR = $(PROJDIR)/tools/test-ca/private
RIAK_CONF = $(RIAK_DIR)/etc/riak.conf
ADV_CONF = $(RIAK_DIR)/etc/advanced.config
RIAK_ADMIN = $(RIAK_DIR)/bin/riak-admin

preconfigure:
	$(TOOLS_DIR)/gen-riak-conf $(RIAK_CONF) 8098 8087
	$(TOOLS_DIR)/strong-consistency-conf $(RIAK_CONF)
	$(TOOLS_DIR)/gen-adv-conf $(ADV_CONF)

configure:
	$(TOOLS_DIR)/riak-cluster-config $(RIAK_ADMIN) 8098 true false

configure-security:
    # Copy certs
	cp $(CERTS_DIR)/cacert.pem $(RESOURCES_DIR)
	cp $(CERTS_DIR)/riak-test-cert.pem $(RESOURCES_DIR)
	cp $(CERTS_DIR)/riakuser-client-cert.pem $(RESOURCES_DIR)

	# PEM Truststore Setup
	openssl pkcs8 -topk8 -inform PEM -outform PEM -in $(PRIVATE_DIR)/riakuser-client-cert-key.pem -out riakuser-client-cert-key_pkcs8.pem -nocrypt
	cp riakuser-client-cert-key_pkcs8.pem $(RESOURCES_DIR)

	# JKS Truststore Setup
	keytool -noprompt -import -trustcacerts -keystore truststore.jks -file $(CERTS_DIR)/cacert.pem -alias cacert -storepass riak123
	keytool -noprompt -import -trustcacerts -keystore truststore.jks -file $(CERTS_DIR)/riak-test-cert.pem -alias servercert -storepass riak123

	keytool -importkeystore -srckeystore $(CERTS_DIR)/riakuser-client-cert.pfx -srcstoretype pkcs12 -srcstorepass '' -destkeystore riak_cert_user.jks -deststorepass riak123 -deststoretype JKS
	keytool -noprompt -import -trustcacerts -keystore riak_cert_user.jks -file $(CERTS_DIR)/cacert.pem -alias cacert -storepass riak123

	cp truststore.jks $(RESOURCES_DIR)
	cp riak_cert_user.jks $(RESOURCES_DIR)

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
