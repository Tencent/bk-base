.PHONY: clean build

all: build

RUN_VERSION=$(RUN_VERSION)
BUILD_NO=$(BUILD_NO)

clean:
	rm .gitlab-ci.yml
	rm .pre-commit-config.yaml
	rm .flake8
	rm -rf ./contents
	rm -rf ./tests/tools
	rm -rf ./tests/dmonitor/data

build:
	echo "Start to build ..."
	rm -rf ./dist
	mkdir -p ./dist
	rsync -r --exclude=dist --exclude=.git ./ ./dist/datamanager
	cd dist && sh datamanager/scripts/build.sh $(RUN_VERSION) $(BUILD_NO)
	echo "Build success."
