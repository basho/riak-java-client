.PHONY: compile test unit-test integration-test protogen

compile:
	mvn clean compile

test: unit-test integration-test

unit-test:
	mvn test

integration-test:
	mvn -Pitest,default -Dcom.basho.riak.2i=true -Dcom.basho.riak.yokozuna=true -Dcom.basho.riak.buckettype=true -Dcom.basho.riak.crdt=true -Dcom.basho.riak.lifecycle=true verify

protogen:
	mvn -Pprotobuf-generate generate-sources
