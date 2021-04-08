.PHONY: all

format:
	@goimports -l -w ./

run-example:
	go run example/main.go