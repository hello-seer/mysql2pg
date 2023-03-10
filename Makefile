###
# Config
###

JOBS ?= $(shell nproc)
MAKEFLAGS += -j $(JOBS) -r

PATH := $(abspath node_modules)/.bin:$(PATH)

.DELETE_ON_ERROR:
.SECONDARY:
.SUFFIXES:

LPAREN := (
RPAREN := )

###
# Clean
###

TARGET := .pytype build mysql2pg.egg-info target

.PHONY: clean
clean:
	rm -fr $(TARGET)

###
# Format
###

FORMAT_SRC := $(shell find . $(TARGET:%=-not \$(LPAREN) -name % -prune \$(RPAREN)) -name '*.py')
PRETTIER_SRC := $(shell find . $(TARGET:%=-not \$(LPAREN) -name % -prune \$(RPAREN)) -name '*.md' -o -name '*.yml')

.PHONY: format
format: target/format.target

.PHONY: test-format
test-format: target/format-test.target

target/format.target: .prettierrc.yml $(FORMAT_SRC) $(PRETTIER_SRC) target/node_modules.target
	mkdir -p $(@D)
	isort --profile black $(FORMAT_SRC)
	black -t py37 $(FORMAT_SRC)
	node_modules/.bin/prettier --write .
	touch $@ target/format-test.target

target/format-test.target: $(FORMAT_SRC)
	mkdir -p $(@D)
	isort -c --profile black $(FORMAT_SRC)
	black -t py37 --check $(FORMAT_SRC)
	touch $@ target/format.target

###
# Npm
###

target/node_modules.target:
	mkdir -p $(@D)
	yarn install
	> $@

###
# Pip
###

PY_SRC := $(shell find . $(TARGET:%=-not \$(LPAREN) -name % -prune \$(RPAREN)) -name '*.py')

.PHONY: install
install:
	pip3 install -e .[dev]

.PHONY: package
package: target/package.target

upload: target/package.target
	TWINE_PASSWORD="$$(aws --output text --query Parameter.Value --region=us-east-1 ssm get-parameter --name /SeerCI/PyPI/Token --with-decryption)" \
		python3 -m twine upload --username=__token__ target/package/*

target/package.target: setup.py README.md $(PY_SRC)
	rm -fr $(@:.target=)
	mkdir -p $(@:.target=)
	./$< bdist_wheel -d $(@:.target=) sdist -d $(@:.target=)
	> $@

###
# Python types
###

.PHONY: test-types
test-types: target/types-test.target

target/types-test.target: setup.cfg $(PY_SRC)
	pytype
	> $@

###
# Test
###

.PHONY: test
test:
	pytest -s test
