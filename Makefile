.DEV_PROFILE := okdata-dev
.PROD_PROFILE := okdata-prod

GLOBAL_PY := python3
BUILD_VENV ?= .build_venv
BUILD_PY := $(BUILD_VENV)/bin/python

.PHONY: init
init: $(BUILD_VENV)

$(BUILD_VENV):
	$(GLOBAL_PY) -m venv $(BUILD_VENV)
	$(BUILD_PY) -m pip install -U pip

.PHONY: format
format: $(BUILD_VENV)/bin/black
	$(BUILD_PY) -m black .

.PHONY: test
test: $(BUILD_VENV)/bin/tox
	$(BUILD_PY) -m tox -p auto -o

.PHONY: upgrade-deps
upgrade-deps: $(BUILD_VENV)/bin/pip-compile
	$(BUILD_VENV)/bin/pip-compile -U

.PHONY: deploy
deploy: init format test login-dev
	@echo "\nDeploying to stage: $${STAGE:-dev}\n"
	sls deploy --stage $${STAGE:-dev} --aws-profile $(.DEV_PROFILE)

.PHONY: deploy-prod
deploy-prod: init format is-git-clean test login-prod
	sls deploy --stage prod --aws-profile $(.PROD_PROFILE)

ifeq ($(MAKECMDGOALS),undeploy)
ifndef STAGE
$(error STAGE is not set)
endif
ifeq ($(STAGE),dev)
$(error Please do not undeploy dev)
endif
endif
.PHONY: undeploy
undeploy: login-dev
	@echo "\nUndeploying stage: $(STAGE)\n"
	sls remove --stage $(STAGE) --aws-profile $(.DEV_PROFILE)

.PHONY: login-dev
login-dev:
ifndef OKDATA_AWS_ROLE_DEV
	$(error OKDATA_AWS_ROLE_DEV is not set)
endif
	saml2aws login --role=$(OKDATA_AWS_ROLE_DEV) --profile=$(.DEV_PROFILE)

.PHONY: login-prod
login-prod:
ifndef OKDATA_AWS_ROLE_PROD
	$(error OKDATA_AWS_ROLE_PROD is not set)
endif
	saml2aws login --role=$(OKDATA_AWS_ROLE_PROD) --profile=$(.PROD_PROFILE)

.PHONY: is-git-clean
is-git-clean:
	@status=$$(git fetch origin && git status -s -b) ;\
	if test "$${status}" != "## main...origin/main"; then \
		echo; \
		echo Git working directory is dirty, aborting >&2; \
		false; \
	fi

.PHONY: build
build: $(BUILD_VENV)/bin/wheel $(BUILD_VENV)/bin/twine
	$(BUILD_PY) setup.py sdist bdist_wheel


###
# Python build dependencies
##

$(BUILD_VENV)/bin/pip-compile: $(BUILD_VENV)
	$(BUILD_PY) -m pip install -U pip-tools

$(BUILD_VENV)/bin/tox: $(BUILD_VENV)
	$(BUILD_PY) -m pip install -I virtualenv==16.7.9
	$(BUILD_PY) -m pip install -U tox

$(BUILD_VENV)/bin/alembic: $(BUILD_VENV)
	$(BUILD_PY) -m pip install -U alembic psycopg2

$(BUILD_VENV)/bin/%: $(BUILD_VENV)
	$(BUILD_PY) -m pip install -U $*


###
# Database migrations
##

.PHONY: generate-migrations
generate-migrations: $(BUILD_VENV)/bin/alembic
	PYTHONPATH=. $(BUILD_VENV)/bin/alembic revision --autogenerate

.PHONY: migrate
migrate: $(BUILD_VENV)/bin/alembic
	PYTHONPATH=. $(BUILD_VENV)/bin/alembic upgrade head
