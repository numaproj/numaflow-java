#!/bin/bash

function show_help () {
    echo "Usage: $0 [-h|--help | -t|--tag <tag>] (-bpe|--build-push-example <execution-id>)"
    echo "  -h, --help                   Display help message and exit"
    echo "  -bpe, --build-push-example   Build the given example id (found in examples/pom.xml), and push it to the quay.io registry"
    echo "  -t, --tag                    To be optionally used with -bpe or -bp. Specify the tag to build with. Default tag: stable"
}

if [ $# -eq 0 ]; then
  echo "Error: provide at least one argument" >&2
  show_help
  exit 1
fi

usingHelp=0
usingBuildPushExample=0
usingTag=0
executionID=""
tag="stable"

function handle_options () {
  while [ $# -gt 0 ]; do
    case "$1" in
      -h | --help)
        usingHelp=1
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

if (( usingBuildPushExample + usingHelp > 1 )); then
  echo "Only one of '-h' or '-bpe' is allowed at a time" >&2
  show_help
  exit 1
fi

if (( (usingTag + usingHelp > 1) || (usingTag && usingBuildPushExample == 0) )); then
  echo "Can only use -t with -bpe" >&2
  show_help
  exit 1
fi

if [ -n "$executionID" ]; then
 echo "Updating example: $executionID"
fi

if [ -n "$tag" ] && (( ! usingHelp )); then
 echo "Using tag: $tag"
fi

executionIDs=("mapt-event-time-filter-function" "flat-map-stream" "map-flatmap" \
              "even-odd" "simple-sink" "reduce-sum" "reduce-stream-sum" \
              "map-forward-message" "reduce-counter" "sideinput-example" \
              "udf-sideinput-example" "source-simple-source" "session-reduce-count"
              )

function dockerPublish () {
  echo "Docker publish for example: $1"
  if ! docker tag numaflow-java-examples/"$1":"$tag" quay.io/numaio/numaflow-java/"$1":"$tag"; then
    echo "Error: failed to tag example $1 with tag $tag" >&2
    exit 1
  fi
  if ! docker push quay.io/numaio/numaflow-java/"$1":"$tag"; then
    echo "Error: failed to push example $1 with tag $tag" >&2
    exit 1
  fi
}

function build () {
  if ! mvn clean install; then
    echo "Error: failed to mvn clean install in root directory" >&2
    exit 1
  fi
  cd examples || exit
  if ! mvn clean install -Ddocker.tag="$tag"; then
    echo "Error: failed to build images in examples directory" >&2
    exit 1
  fi
}

if (( usingBuildPushExample )); then
  build
  dockerPublish "$executionID"
elif (( usingHelp )); then
  show_help
fi
