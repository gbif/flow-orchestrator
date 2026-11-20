#!/bin/sh
# Builds an image inside minikube from a runnable JAR, so it has minikube's own architecture (arm64
# on Apple Silicon, where the published amd64 images fail with "exec format error"):
#   build-image.sh REPO_DIR JAR_GLOB DOCKERFILE IMAGE
# JAR_GLOB and DOCKERFILE are relative to REPO_DIR; run "mvn package" there first. The Dockerfile
# must take ARG BASE_REGISTRY and ARG JAR_FILE and copy docker/entrypoint.sh, as this repository's
# and dwc-dp-analyser-service's do. The build context holds only the Dockerfile, the entrypoint and
# the JAR, so nothing else of the repository is sent to minikube.
# BASE_REGISTRY: where eclipse-temurin:17-jre comes from (default docker.gbif.org/third-party;
# docker.io/library works without GBIF's registry). KUBE_CONTEXT: the minikube profile.
set -eu

repo="$1"
jar_glob="$2"
dockerfile="$3"
image="$4"
base_registry="${BASE_REGISTRY:-docker.gbif.org/third-party}"

cd "$repo"
# shellcheck disable=SC2086 # the glob must expand
set -- $jar_glob
if [ "$#" -ne 1 ] || [ ! -f "$1" ]; then
  echo "Expected exactly one $repo/$jar_glob; run mvn package there first." >&2
  exit 1
fi
jar="$1"

context=$(mktemp -d)
trap 'rm -rf "$context"' EXIT
mkdir -p "$context/docker" "$context/target"
cp docker/entrypoint.sh "$context/docker/"
cp "$jar" "$context/target/service.jar"

# No build args: minikube hands --build-opt to the runtime's builder (docker build, buildctl,
# podman), and each spells them differently. The staged Dockerfile gets the values as ARG
# defaults instead, and loses its "# syntax=" line so BuildKit does not pull a frontend image.
sed -e '/^# syntax=/d' \
  -e "s|^ARG BASE_REGISTRY=.*|ARG BASE_REGISTRY=$base_registry|" \
  -e 's|^ARG JAR_FILE$|ARG JAR_FILE=target/service.jar|' \
  "$dockerfile" >"$context/Dockerfile"
grep -q '^ARG JAR_FILE=target/service.jar$' "$context/Dockerfile" || {
  echo "$repo/$dockerfile changed: could not set the JAR_FILE default" >&2
  exit 1
}

echo "Building $image from $repo/$jar (base registry $base_registry)"
minikube -p "${KUBE_CONTEXT:-minikube}" image build -t "$image" -f Dockerfile "$context"
