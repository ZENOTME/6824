go build -race -buildmode=plugin ../mrapps/wc.go 
go run -race mrworker.go wc.so &
go run -race mrworker.go wc.so &
go run -race mrworker.go wc.so &

