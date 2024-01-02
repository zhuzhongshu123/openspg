# for amd64
docker build -f Dockerfile --platform linux/amd64 --push \
  -t openspg/openspg-mysql-amd64:0.0.1-beta0101 \
  -t openspg/openspg-mysql-amd64:latest \
  .

# for arm64-v8
docker build -f Dockerfile --platform linux/arm64/v8 --push \
  -t openspg/openspg-mysql-arm64v8:0.0.1-beta0101 \
  -t openspg/openspg-mysql-arm64v8:latest \
  .
