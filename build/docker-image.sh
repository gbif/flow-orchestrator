#!/bin/bash -e

IMAGE=$1
BASE_REGISTRY=${2:-docker.gbif.org/third-party}

# The app module produces both a plain JAR and a runnable shaded JAR.
# Only the shaded JAR is packaged in the Docker image.
JAR_DIR="flow-orchestrator-app/target"
JAR_PATTERN="${JAR_DIR}/*-shaded.jar"

mapfile -t JARS < <(compgen -G "${JAR_PATTERN}" || true)

if [[ ${#JARS[@]} -eq 0 ]]; then
  echo >&2 "ERROR: Could not find ${JAR_PATTERN}"
  echo >&2 "Run 'mvn package' before building the Docker image."
  exit 1
fi

if [[ ${#JARS[@]} -gt 1 ]]; then
  echo >&2 "ERROR: Found multiple shaded JARs:"
  printf >&2 '  %s\n' "${JARS[@]}"
  echo >&2 "Run 'mvn clean package' to produce an unambiguous build."
  exit 1
fi

JAR_FILE="${JARS[0]}"

echo "Building Docker image: ${IMAGE}"
echo "Using JAR: ${JAR_FILE}"
echo "Using base registry: ${BASE_REGISTRY}"

docker build \
  --build-arg BASE_REGISTRY="${BASE_REGISTRY}" \
  --build-arg JAR_FILE="${JAR_FILE}" \
  -f docker/Dockerfile \
  -t "${IMAGE}" \
  .
