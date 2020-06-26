## Requirements
- [golang protobuf](https://pkg.go.dev/mod/google.golang.org/protobuf)
- [golang grpc](https://pkg.go.dev/google.golang.org/grpc?tab=doc)
- [protoc-go-inject-tag](https://github.com/favadi/protoc-go-inject-tag)
- [golang mongodb](https://docs.mongodb.com/drivers/go/)

To generate code from tradepb.proto, run the command `protoc tradepb.proto --go_out=plugins=grpc:.` in the tradepb folder.  After generating the file, run the command `protoc-go-inject-tag -input=./tradepb.pb.go` to inject the necessary tags into the code.