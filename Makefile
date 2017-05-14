.PHONY: test coverage format clean

PY_FILES = $(shell find . -name '*.py')

test:
	python -m pytest

.coverage: 
	coverage run --source vote_on_articles -m py.test

coverage: .coverage
	coverage html --omit="*/test*" 
	open htmlcov/index.html

format:
	yapf --in-place $(PY_FILES)

clean:
	rm -f .coverage
	rm -rf htmlcov
