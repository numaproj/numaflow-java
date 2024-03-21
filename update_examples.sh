#!/bin/bash

function show_help () {
    echo "Usage: $0 [-h|--help | -t|--tag <tag>] (-bp|--build-push | -bpe|--build-push-example <execution-id> | -u|--update <SDK-version>)"
    echo "  -h, --help                   Display help message and exit"
    echo "  -bp, --build-push            Build all the examples and push them to the quay.io registry"
    echo "  -bpe, --build-push-example   Build the given example id (found in examples/pom.xml), and push it to the quay.io registry"
    echo "  -t, --tag                    To be optionally used with -bpe or -bp. Specify the tag to build with. Default tag: stable"
    echo "  -u, --update                 Update the pom.xml files in the root and example directories to the specified version"
}

if [ $# -eq 0 ]; then
  echo "Error: provide at least one argument" >&2
  show_help
  exit 1
fi

usingHelp=0
usingBuildPush=0
usingBuildPushExample=0
usingVersion=0
usingTag=0
version=""
executionID=""
tag="stable"

function handle_options () {
  while [ $# -gt 0 ]; do
    case "$1" in
      -h | --help)
        usingHelp=1
        ;;
      -bp | --build-push)
        usingBuildPush=1
        ;;
      -bpe | --build-push-example)
        if [ -z "$2" ]; then
          echo "execution ID of example not specified." >&2
          show_help
          exit 1
        fi

        usingBuildPushExample=1
        executionID=$2
        shift
        ;;
      -t | --tag)
        if [ -z "$2" ]; then
          echo "Tag not specified." >&2
          show_help
          exit 1
        fi

        usingTag=1
        tag=$2
        shift
        ;;
      -u | --update)
        if [ -z "$2" ]; then
          echo "Version not specified." >&2
          show_help
          exit 1
        fi

        usingVersion=1
        version=$2
        shift
        ;;
      *)
        echo "Invalid option: $1" >&2
        show_help
        exit 1
        ;;
    esac
    shift
  done
}

handle_options "$@"

if (( usingBuildPush + usingBuildPushExample + usingHelp + usingVersion > 1 )); then
  echo "Only one of '-h', '-bp', '-bpe', or '-u' is allowed at a time" >&2
  show_help
  exit 1
fi

if (( (usingTag + usingHelp + usingVersion > 1) || (usingTag && usingBuildPush + usingBuildPushExample == 0) )); then
  echo "Can only use -t with -bp or -bpe" >&2
  show_help
  exit 1
fi

if [ -n "$version" ]; then
 echo "Using version: $version"
fi

if [ -n "$executionID" ]; then
 echo "Updating example: $executionID"
fi

if [ -n "$tag" ] && (( ! usingHelp )) && (( ! usingVersion )); then
 echo "Using tag: $tag"
fi

executionIDs=("mapt-event-time-filter-function" "flat-map-stream" "map-flatmap" \
              "even-odd" "simple-sink" "reduce-sum" "reduce-stream-sum" \
              "map-forward-message" "reduce-counter" "sideinput-example" \
              "udf-sideinput-example" "source-simple-source" "session-reduce-count"
              )

function dockerPublish () {
  echo "Docker publish for example: $1"
  if ! docker tag numaflow-java-examples/"$1":latest quay.io/numaio/numaflow-java/"$1":"$2"; then
    echo "Error: failed to tag example $1 with tag $2" >&2
    exit 1
  fi
  if ! docker push quay.io/numaio/numaflow-java/reduce-sum:"$2"; then
    echo "Error: failed to push example $1 with tag $2" >&2
    exit 1
  fi
}

if (( usingBuildPush )); then
  if ! mvn clean install; then
    echo "Error: failed to mvn clean install in root directory" >&2
    exit 1
  fi
  cd examples || exit
  if ! mvn clean install; then
    echo "Error: failed to build images in examples directory" >&2
    exit 1
  fi
  for id in "${executionIDs[@]}"
  do
    dockerPublish "$id" "$tag"
  done
elif (( usingBuildPushExample )); then
  if ! mvn clean install; then
    echo "Error: failed to mvn clean install in root directory" >&2
    exit 1
  fi
  cd examples || exit
  if ! mvn jib:dockerBuild@"$executionID"; then
    echo "Error: failed to build example image $executionID" >&2
    exit 1
  fi
  dockerPublish "$executionID" "$tag"
elif (( usingVersion )); then
  if ! mvn versions:set -DnewVersion="$version"; then
    echo "Error: failed to update version in pom.xml file in root directory" >&2
    exit 1
  fi

  cd examples || exit
  if ! mvn versions:use-dep-version -Dincludes=io.numaproj.numaflow -DdepVersion="$version" -DforceVersion=true; then
    echo "Error: failed to update version in pom.xml file in examples directory" >&2
    exit 1
  fi
elif (( usingHelp )); then
  show_help
fi
