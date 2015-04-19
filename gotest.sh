cd base/
go fmt *.go && go test
cd ..

cd sinks/splunk
go fmt *.go && go test
cd ../..

cd sinks/kafka
go fmt *.go && go test
cd ../..

cd sources/snow
go fmt *.go && go test
cd ../..

cd sources/kafka
go fmt *.go && go test
cd ../..
