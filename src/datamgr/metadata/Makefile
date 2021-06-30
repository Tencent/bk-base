
.PHONY: clean build

all: build

FAMILY=$(FAMILY)
RUN_VERSION=$($RUN_VERSION)

build:
	echo "Start to build ..."
	rm -rf ./dist
	mkdir -p ./dist
	rsync -r --exclude=dist --exclude=.git ./ ./dist/metadata
	cd dist && sh metadata/scripts/build.sh $(RUN_VERSION) $(BUILD_NO)
	echo "Build success."

clean:
	rm .gitlab-ci.yml
	rm .flake8
	rm .gitignore
	rm pytest.ini
	rm pyproject.toml
	rm babel.cfg
	rm local_requirements.txt
	rm requirements.in
	rm -rf ./tests
