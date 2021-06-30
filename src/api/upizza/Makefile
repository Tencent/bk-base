.PHONY: clean build

all: build

SUBMODULE=$(MODULE)
RUN_VERSION=$(RUN_VERSION)
BUILD_NO=$(BuildNo)

clean:
	rm .gitlab-ci.yml
	rm .pre-commit-config.yaml
	rm .flake8
	rm -rf ./.gitmodules
	rm -rf ./auth
	rm -rf ./codecheck
	rm -rf ./datacube
	rm -rf ./dataflow
	rm -rf ./datahub
	rm -rf ./datalab
	rm -rf ./datamanage
	rm -rf ./dataquery
	rm -rf ./jobnavi
	rm -rf ./meta
	rm -rf ./old
	rm -rf ./resourcecenter
	rm -rf ./tdw
	rm -rf ./contents
build:
	@echo "Start to build ..."
	rm -rf ./dist
	mkdir -p ./dist
	rsync -r --exclude=dist --exclude=.git ./ ./dist/$(SUBMODULE)api
	rsync -r ../$(SUBMODULE)/ ./dist/$(SUBMODULE)api/$(SUBMODULE)
	cd dist && sh $(SUBMODULE)api/scripts/build.sh $(SUBMODULE) $(RUN_VERSION) $(BUILD_NO)