#!/usr/bin/env bash

VERSION="1-prerelease"

if [[ "$(docker images -q "local/mnp-gw:$VERSION" 2> /dev/null)" == "" ]]; then
    # Here we build the docker image locally as "local/mnp-gw:$VERSION"
    docker build --progress=plain --build-arg TARGETARCH=amd64 -t "local/mnp-gw:$VERSION" "${0%/*}/../"

    # Here we push to Artifactory
    docker tag "local/mnp-gw:$VERSION" "artifactory.qvantel.net/mnp-gw:$VERSION"    
    docker push "artifactory.qvantel.net/mnp-gw:$VERSION"
fi

helm upgrade --install mnp -n mnp --create-namespace "${0%/*}/mnp-gw" \
--set imageVersion=$VERSION