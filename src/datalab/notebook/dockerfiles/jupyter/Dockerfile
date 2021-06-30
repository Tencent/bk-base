# Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.
#
# Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
#
# BK-BASE 蓝鲸基础平台 is licensed under the MIT License.
#
# License for BK-BASE 蓝鲸基础平台:
# --------------------------------------------------------------------
# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
# documentation files (the "Software"), to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
# and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all copies or substantial
# portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
# LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
# NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

FROM python:3.7.5

ARG NB_USER=datalab
ARG NB_UID=1000
ARG NB_GID=100

ARG NOTEBOOK_VERSION=git+https://github.com/BKBASE-Plugin/notebook.git@bkbase
ARG JUPYTERHUB_VERSION=git+https://github.com/BKBASE-Plugin/jupyterhub.git@bkbase

ENV HOME=/home/$NB_USER

COPY src/datalab/notebook/dockerfiles/jupyter/requirements.txt $HOME/dependency/requirements.txt
# 用户notebook存放的路径
COPY src/datalab/notebook/dockerfiles/notebooks $HOME/notebooks
COPY src/datalab/notebook/notebook-command $HOME/dependency/notebook-command
COPY VERSION $HOME/dependency/notebook-command/VERSION

RUN apt-get update && \
    apt-get install -y npm git-all graphviz && \
    npm install bower

RUN pip3 install ${NOTEBOOK_VERSION} ${JUPYTERHUB_VERSION} nbgitpuller==0.10.0 && \
	jupyter serverextension enable --py nbgitpuller --sys-prefix
RUN pip3 install $HOME/dependency/notebook-command/ && \
	rm -rf $HOME/dependency/notebook-command
RUN pip3 install --no-cache-dir -r $HOME/dependency/requirements.txt
    
# 安装扩展插件：执行耗时插件和代码补全插件
RUN pip3 install jupyter_contrib_nbextensions && \
	jupyter contrib nbextension install && \
	jupyter nbextension enable execute_time/ExecuteTime && \
	jupyter nbextension enable hinterland/hinterland

# 设置ipython启动时自动加载command模块，无需执行 %load_ext command
COPY src/datalab/notebook/dockerfiles/jupyter/ipython_config.py $HOME/.ipython/profile_default/

RUN useradd -m $NB_USER
ENV PATH=/usr/local/python3/bin:$PATH

RUN wget https://github.com/Tencent/TencentKona-8/releases/download/8.0.4-GA/TencentKona8.0.4.b11_jdk_linux-x86_64_8u272.tar.gz && \
    tar -xvf TencentKona8.0.4.b11_jdk_linux-x86_64_8u272.tar.gz -C $HOME/dependency/ && \
    rm TencentKona8.0.4.b11_jdk_linux-x86_64_8u272.tar.gz
ENV JAVA_HOME=$HOME/dependency/TencentKona-8.0.4-272
ENV PATH $JAVA_HOME/bin:$PATH
ENV CLASSPATH .:$JAVA_HOME/lib:$HOME/dependency/libs/*
ENV JAVA_OPTS="-Xms256m -Xmx2048m\
    -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$HOME/logs/`date +%Y%m%d%H%M%S`_dump.log"

RUN chown -R $NB_USER:$NB_USER $HOME
WORKDIR $HOME
USER $NB_USER

EXPOSE 8888

