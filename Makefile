.PHONY: clean test build release install

clean:
	@rm -rf build

test:
	@go test ./... -count=1

build: clean
	GOOS=linux GOARCH=amd64 go build -o build/tr 
	chmod 0644 build/tr
	zip -j build/tr.zip build/tr

release: build
	@if [ -z "$(bucket)" ]; then \
		echo "bucket argument is required"; \
		exit 1; \
	fi
	@if [ -z "$(key)" ]; then \
		echo "key argument is required"; \
		exit 1; \
	fi
	$(eval VERSION=$(shell git rev-parse HEAD | cut -c1-8))
	mv build/tr.zip "build/tr_$(VERSION).zip"	
	cp cfn-stack.yaml build/cfn-stack-$(VERSION).yaml
	aws s3 cp "build/tr_$(VERSION).zip" "s3://$(bucket)/$(key)/tr_$(VERSION).zip"
	aws s3 cp cfn-stack.yaml "s3://$(bucket)/$(key)/cfn-stack-$(VERSION).yaml"
	@echo "Release s3://$(bucket)/$(key)/tr_$(VERSION).zip"

deploy: release
	$(eval VERSION=$(shell git rev-parse HEAD | cut -c1-8))
	@if [ -z "$(security-group-ids)" ]; then \
		echo "security-group-ids argument is required"; \
		exit 1; \
	fi
	@if [ -z "$(subnet-ids)" ]; then \
		echo "subnet-ids argument is required"; \
		exit 1; \
	fi
	aws cloudformation deploy --template-file build/cfn-stack-$(VERSION).yaml --stack-name tr --capabilities=CAPABILITY_IAM --parameter-overrides S3Bucket=$(bucket) S3Key="$(key)/tr_$(VERSION).zip" SecurityGroupIds=$(security-group-ids) SubnetIds=$(subnet-ids)
	aws cloudformation describe-stacks --stack-name tr --query "Stacks[].Outputs" | cat