.PHONY: all

format:
	@goimports -l -w ./

run-example:
	go build -o rmq-example example/main.go
	./rmq-example