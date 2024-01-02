docker buildx build -f Dockerfile --platform linux/arm64/v8,linux/amd64 --push \
  -t openspg/openspg-mysql:0.0.1-beta0101 \
  -t openspg/openspg-mysql:latest \
  .
