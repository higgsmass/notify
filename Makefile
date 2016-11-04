#!/usr/bin/make -f

BUILD_NUMBER := $(shell date +%Y%m%d%H%M%S)
PKG_NAME := notify


.PHONY: help
help:
	@echo "clean-build - cleans up build artifacts"
	@echo "clean-pkg - removes previously created packages"
	@echo "clean-pyc - removes bytecompiled python files"
	@echo "help - prints this message"
	@echo "prepare - prepares the package for building"
	@echo "pytest-unit - executes python unit tests"
	@echo "release - creates a release distribution"
	@echo "vagrant-test - executes all integration tests"


.PHONY: clean-build
clean-build:
	@echo "--> Cleans up build artifacts"
	@rm -rf .tox/
	@rm -rf build/
	@rm -rf dist/
	@rm -rf *.egg-info
	@rm -rf *.egg

.PHONY: clean-pkg
clean-pkg:
	@echo "--> Cleans up any pre-created packages"
	@find ../ -maxdepth 1 -iname '$(PKG_NAME)_*_amd64.changes' -print0 | xargs -0 rm -f +
	@find ../ -maxdepth 1 -iname '$(PKG_NAME)_*_amd64.deb' -print0 | xargs -0 rm -f +
	@find ../ -maxdepth 1 -iname '$(PKG_NAME)_*.dsc' -print0 | xargs -0 rm -f +
	@find ../ -maxdepth 1 -iname '$(PKG_NAME)_*.tar.gz' -print0 | xargs -0 rm -f +


.PHONY: clean-pyc
clean-pyc:
	@echo "--> Removes unnecessary Python bytefiles"
	@find . -iname '*.pyc' -print0 | xargs -0 rm -fv +
	@find . -iname '*.pyo' -print0 | xargs -0 rm -fv +
	@find . -iname '*~' -print0 | xargs -0 rm -fv +


.PHONY: pytest-unit
pytest-unit: prepare
	@echo "--> Running unit tests"
	@python setup.py test

release: prepare
	@echo "--> Creating a release"
	@python setup.py sdist


.PHONY: vagrant-test
vagrant-test:
	@echo "--> Executes tests in vagrant"
	@vagrant destroy -f
	@vagrant box update
	@vagrant up
	@vagrant ssh -c 'sudo make -C "~/vagrant" pytest-unit'
	@vagrant destroy -f


.PHONY: prepare
prepare: clean-pkg clean-build clean-pyc
	@echo "--> Prepared the build environment for execution."

