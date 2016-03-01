
PROJDIR = $(realpath $(CURDIR)/..)
RESOURCES_DIR = $(PROJDIR)/src/test/resources/
TOOLS_DIR = $(PROJDIR)/tools/devrel
CA_DIR = $(PROJDIR)/tools/test-ca
CERTS_DIR = $(CA_DIR)/certs
PRIVATE_DIR = $(CA_DIR)/private
RIAK_CONF = $(RIAK_DIR)/etc/riak.conf
ADV_CONF = $(RIAK_DIR)/etc/advanced.config
RIAK_ADMIN = $(RIAK_DIR)/bin/riak-admin

preconfigure: configure-security-certs
	$(TOOLS_DIR)/gen-riak-conf $(RIAK_CONF) 8098 8087 18098 $(CERTS_DIR)/cacert.pem $(CERTS_DIR)/riak-test-cert.pem $(PRIVATE_DIR)/riak-test-key.pem
	$(TOOLS_DIR)/gen-adv-conf $(ADV_CONF)

configure:
	$(TOOLS_DIR)/riak-cluster-config $(RIAK_ADMIN) 8098 true true

configure-security-certs:
	mkdir -p $(RESOURCES_DIR)
	# Copy certs
	cp $(CERTS_DIR)/cacert.pem $(RESOURCES_DIR)
	cp $(CERTS_DIR)/riak-test-cert.pem $(RESOURCES_DIR)
	cp $(CERTS_DIR)/riakuser-client-cert.pem $(RESOURCES_DIR)

	# PEM Truststore Setup
	openssl pkcs8 -topk8 -inform PEM -outform PEM -in $(PRIVATE_DIR)/riakuser-client-cert-key.pem -out riakuser-client-cert-key_pkcs8.pem -nocrypt
	mv riakuser-client-cert-key_pkcs8.pem $(RESOURCES_DIR)

	# JKS Truststore Setup
	keytool -noprompt -import -trustcacerts -keystore truststore.jks -file $(CERTS_DIR)/cacert.pem -alias cacert -storepass riak123
	keytool -noprompt -import -trustcacerts -keystore truststore.jks -file $(CERTS_DIR)/riak-test-cert.pem -alias servercert -storepass riak123

	keytool -importkeystore -srckeystore $(CERTS_DIR)/riakuser-client-cert.pfx -srcstoretype pkcs12 -srcstorepass '' -destkeystore riak_cert_user.jks -deststorepass riak123 -deststoretype JKS
	keytool -noprompt -import -trustcacerts -keystore riak_cert_user.jks -file $(CERTS_DIR)/cacert.pem -alias cacert -storepass riak123

	mv truststore.jks $(RESOURCES_DIR)
	mv riak_cert_user.jks $(RESOURCES_DIR)

compile:
	cd ..;
	mvn clean compile

lint:
	@echo "No lint task"

test: test-normal test-security

test-normal:
	$(RIAK_ADMIN) security disable
	cd ..;
	mvn -Pitest,default -Dcom.basho.riak.2i=true -Dcom.basho.riak.yokozuna=true -Dcom.basho.riak.buckettype=true -Dcom.basho.riak.crdt=true verify

test-security:
	$(RIAK_ADMIN) security enable
	cd ..;
	mvn -Pitest,test-debug-logging  -Dcom.basho.riak.security=true -Dcom.basho.riak.security.clientcert=true test-compile failsafe:integration-test