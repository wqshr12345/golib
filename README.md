## Local-to-cloud Database Synchronization via Fine-grained Hybrid Compression

**1 Introduction**

This is the code implemented according to the paper——Local-to-cloud Database Synchronization via Fine-grained Hybrid Compression

**2. Compile**

```
git clone https://github.com/wqshr12345/golib.git
cd golib
go get -u && go mod tidy
cd example
go build example.go -o example
unzip test_data.zip
```
**3. Run**

Some parameters need to be given at runtime. The following is an explanation about the parameters.
```
rate // the network bandwidth used in this example.
generationTime // the generation time(ms) for binlog used in this example.
cmprThread  // thread numbers for compression.
blockSize // the block size(MB) which partitioner used.
typeName // the compression method used for the system(normal means ours design).
isFull // whether to enable binlog specific transformation.

```
Run a simple example.
```
./example -rate=100 -generationTime=10 -cmprThread=1  -blockSize=10 -typeName=normal -isFull=false
```
**4. Run the benchmark**
```
cd example
go build example.go -o example
bash test_10ms.sh
bash test_100ms.sh
bash test_1000ms.sh
```