task :default => :test

#we are ${DIR}/src/github.com/shenfeng/rockredis, we want to point to $DIR
pwd = Dir.pwd
pwd = File.absolute_path("#{pwd}/../../../..")

rock = "#{pwd}/deps/rocksdb"
ENV["GOPATH"] = pwd
ENV["LD_LIBRARY_PATH"] = rock
# osx
ENV["DYLD_LIBRARY_PATH"] = rock

task :test do
  #    system({"GOPATH" => dir, "LD_LIBRARY_PATH" => "deps/rocksdb"}, "ls; env")
  sh "mkdir -p tmp && go test . -cover -coverprofile tmp/cover.out -covermode count "
  sh "go tool cover -html=tmp/cover.out -o tmp/coverage.html"
end

task :bench do
  puts "mkdir -p tmp && (GOPATH=#{pwd} LD_LIBRARY_PATH=#{rock} DYLD_LIBRARY_PATH=#{rock} go test -bench . -benchmem -cpuprofile cpu.out -memprofile mem.out -benchtime 5s -outputdir tmp)"
  # go tool pprof src/src.test tmp/cpu.out
  # go tool pprof src/src.test tmp/mem.out
  sh "mkdir -p tmp && go test -bench . -benchmem -cpuprofile cpu.out -memprofile mem.out -outputdir tmp"
end

task :deps do
  sh "./scripts/deps.sh"
end

task :run do
  sh "go build -o rockredis && ./rockredis -conf ./rockredis.conf"
end


task :env do
  puts "GOPATH=#{pwd} LD_LIBRARY_PATH=#{rock} DYLD_LIBRARY_PATH=#{rock}"
end
