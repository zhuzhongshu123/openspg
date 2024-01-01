# for amd64
docker build -f Dockerfile --platform linux/amd64 --push \
  -t openspg/openspg-python-amd64:0.0.2-beta1 \
  -t openspg/openspg-python-amd64:latest \
  .

# for arm64-v8
docker build -f Dockerfile --platform linux/arm64/v8 --push \
  -t openspg/openspg-python-arm64v8:0.0.2-beta1 \
  -t openspg/openspg-python-arm64v8:latest \
  .
