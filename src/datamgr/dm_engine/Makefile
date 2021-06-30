.PHONY: clean build

all: build

clean:
	rm .gitlab-ci.yml
	rm .pre-commit-config.yaml
	rm .flake8
	rm -rf ./contents
	rm -rf ./tests/tools

build:
	echo "Start to build ..."
	rm -rf ./dist
	mkdir -p ./dist
	rsync -r --exclude=dist --exclude=.git ./ ./dist/dm_engine
	python3 setup.py sdist
