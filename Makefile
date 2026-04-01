.PHONY: install test lint package clean deploy

install:
	pip install -r requirements.txt

test:
	pytest tests/ -v --cov=. --cov-report=term-missing

lint:
	flake8 . --max-line-length=120 --exclude=.venv,venv,.git,__pycache__,*.egg-info
	black --check . --exclude '/(\.venv|venv|\.git|__pycache__)/'

format:
	black . --exclude '/(\.venv|venv|\.git|__pycache__)/'

package:
	zip -r dist/pipeline.zip . \
		--exclude "*.git*" \
		--exclude "*.venv*" \
		--exclude "*__pycache__*" \
		--exclude "*.pytest_cache*" \
		--exclude "dist/*"

deploy:
	aws cloudformation deploy \
		--stack-name olist-de-pipeline \
		--template-file infra/cloudformation/template.yml \
		--region ap-south-1 \
		--capabilities CAPABILITY_NAMED_IAM

clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache .coverage htmlcov dist/
