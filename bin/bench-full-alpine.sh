# Hyperfine as a benchmark driver, `gcompat` to execute the glibc-based go binaries in Alpine.
apk add hyperfine gcompat
echo "Benchmarking 10,000,000 parallel writes and read with the hyperfine utility..."
# shellcheck disable=SC2016
hyperfine 'TOPIC=test-$RANDOM; ./build/publish --no-logs --topic $TOPIC & ./build/consume --no-logs --topic $TOPIC & wait'
