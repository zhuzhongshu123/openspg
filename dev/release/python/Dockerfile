# Copyright 2023 Ant Group CO., Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
# in compliance with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied.

FROM ubuntu:20.04

# For apt to be noninteractive
ENV DEBIAN_FRONTEND noninteractive
ENV DEBCONF_NONINTERACTIVE_SEEN true
ENV LANG C.UTF-8

ARG TARGETPLATFORM
ARG APT_INSTALL="apt-get install --no-install-recommends -y"

ARG PIP_PKGS="openspg-knext==0.0.2-beta1 openspg-nn4k==0.0.2-beta1"

RUN apt-get clean && apt-get update && \
    # Install openjdk 8.
    $APT_INSTALL openjdk-8-jdk && \
    update-alternatives --set java /usr/lib/jvm/java-8-openjdk-`echo $TARGETPLATFORM | cut -d'/' -f2`/jre/bin/java && \
    # Install build / source control tools
    $APT_INSTALL wget git maven less vim && \
    # Install needed python packages. Use pip for installing packages (for consistency).
    $APT_INSTALL python3.8 python3-pip

# It is specified here to install all packages in /usr/lib/python3.8/site-packages path,
# otherwise pemja will report an error because the package cannot be found.
RUN pip3 install --target=/usr/lib/python3.8/site-packages $PIP_PKGS

RUN knext config set --global graph_store_url="tugraph://tugraph:9090?graphName=default&timeout=50000&accessId=admin&accessKey=73@TuGraph" && \
    knext config set --global search_engine_url="elasticsearch://elasticsearch:9200?scheme=http" && \
    knext config set --global host_addr="http://server:8887"

RUN git clone --depth=1 https://github.com/OpenSPG/openspg.git