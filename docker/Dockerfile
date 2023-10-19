FROM ubuntu:18.04

RUN echo 'root:root' |chpasswd

RUN apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# install basic libraries
RUN apt-get update
RUN apt-get install -y git vim htop sudo make cmake gcc gdb g++ strace silversearcher-ag net-tools iperf3 nload iproute2 wget

# copy .gitconfig
COPY gitconfig /root/.gitconfig

# deploy Rolis environment
WORKDIR "/root"
# using branch "master" for the Rolis
RUN git clone --branch crpc_test https://github.com/akshat2602/rolis.git

WORKDIR "/root/rolis"
# install dependencies
RUN bash install.sh