install:
	pip install --upgrade pip &&\
	pip install -r requirements.txt

test:
	python -m pytest -vv -cov=mylib test_*.py

format:
	black *.py

lint:
	ruff check --line-length 120 *.py mylib/*.py test_*.py

run:
	python main.py

container-lint:
	docker run --rm -i hadolint/hadolint < Dockerfile

generate_and_push:
	# Add, commit, and push the generated files to GitHub
	@if [ -n "$$(git status --porcelain)" ]; then \
		git config --local user.email "action@github.com"; \
		git config --local user.name "GitHub Action"; \
		git add .; \
		git commit -m "Add output log"; \
		git push; \
	else \
		echo "No changes to commit. Skipping commit and push."; \
	fi
	
all: install format test lint run