.PHONY: build

build:
	mvn clean compile && \
	rm -rf deploy_package/classes && \
	cp -r target/classes deploy_package
