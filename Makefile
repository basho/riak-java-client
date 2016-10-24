.PHONY: all clean compile protogen configure-security-certs help
.PHONY: test unit-test integration-test
.PHONY: integration-test-timeseries integration-test-hll integration-test-security

PROJDIR = $(realpath $(CURDIR))
RESOURCES_DIR = $(PROJDIR)/src/test/resources/
CA_DIR = $(PROJDIR)/tools/test-ca
CERTS_DIR = $(CA_DIR)/certs
PRIVATE_DIR = $(CA_DIR)/private
RIAK_PORT ?= 808f
RUN_YOKOZUNA ?= true

all: test

clean:
	mvn clean

compile:
	mvn compile

test: unit-test integration-test

test-ts: unit-test integration-test-timeseries

unit-test:
	mvn test

integration-test:
	mvn -Pitest,default -Dcom.basho.riak.yokozuna=$(RUN_YOKOZUNA) -Dcom.basho.riak.pbcport=$(RIAK_PORT) verify

integration-test-hll:
	mvn -Pitest,default -Dcom.basho.riak.yokozuna=false -Dcom.basho.riak.pbcport=$(RIAK_PORT) -Dcom.basho.riak.hlldt=true verify

integration-test-timeseries:
	mvn -Pitest,default -Dcom.basho.riak.yokozuna=false -Dcom.basho.riak.timeseries=true -Dcom.basho.riak.pbcport=$(RIAK_PORT) verify

integration-test-security: configure-security-certs
	mvn -Pitest,default -Dcom.basho.riak.yokozuna=$(RUN_YOKOZUNA) -Dcom.basho.riak.security=true -Dcom.basho.riak.security.clientcert=true -Dcom.basho.riak.pbcport=$(RIAK_PORT) test-compile failsafe:integration-test

protogen:
	mvn -Pprotobuf-generate generate-sources

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

	mv -f truststore.jks $(RESOURCES_DIR)
	mv -f riak_cert_user.jks $(RESOURCES_DIR)

help:
	@echo ''
	@echo ' Targets:'
	@echo '-----------------------------------------------------------------'
	@echo ' all                          - Run everything                   '
	@echo ' lint                         - Run jshint                       '
	@echo ' install-deps                 - Install required dependencies    '
	@echo ' test                         - Run unit & integration tests     '
	@echo ' unit-test                    - Run unit tests                   '
	@echo ' integration-test-hll         - Run HLL integration tests        '
	@echo ' integration-test-timeseries  - Run TS integration tests         '
	@echo ' integration-test-security    - Run security tests               '
	@echo '-----------------------------------------------------------------'
	@echo ''
