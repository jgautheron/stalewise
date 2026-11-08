.PHONY: test-chainsaw
test-chainsaw:
	docker build -t stalewise:tests .
	bash scripts/chainsaw.sh
