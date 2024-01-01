# for amd64
docker build -f Dockerfile --platform linux/amd64 --push \
  -t openspg/openspg-server-amd64:0.0.2-beta1 \
  -t openspg/openspg-server-amd64:latest \
  .

# for arm64-v8
docker build -f Dockerfile --platform linux/arm64/v8 --push \
  -t openspg/openspg-server-arm64v8:0.0.2-beta1 \
  -t openspg/openspg-server-arm64v8:latest \
  .
