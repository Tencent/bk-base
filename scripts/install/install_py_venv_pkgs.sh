#!/usr/bin/env bash

set -eo pipefail

# 通用脚本框架变量
PROGRAM=$(basename "$0")
VERSION=1.0
EXITCODE=0

# 全局默认变量
#PYTHON_PATH=/opt/py36/bin/python
PIP_OPTIONS=(--no-cache-dir)
ENCRYPT=

# error exit handler
err_trap_handler () {
    MYSELF="$0"
    LASTLINE="$1"
    LASTERR="$2"
    echo "${MYSELF}: line ${LASTLINE} with exit code ${LASTERR}" >&2
}
trap 'err_trap_handler ${LINENO} $?' ERR

usage () {
    cat <<EOF
用法: 
    $PROGRAM [ -h --help -?  查看帮助 ]
            [ -n, --virtualenv  [必选] "指定virtualenv名字" ]
            [ -w, --workon-home [可选] "指定WORKON_HOME路径，默认为\$HOME/.virtualenvs" ]
            [ -p, --python-path [可选] "指定python的路径，默认为/opt/py27/bin/python" ]
            [ -a, --project-home [可选] "指定python工程的家目录" ]
            [ -s, --pkg-path    [可选] "指定本地包路径" ]
            [ -r, --req-file    [必选] "指定requirements.txt的路径" ]
            [ -e, --encrypt     [可选] "指定的python解释器为加密解释器" ]
            [ -v, --version     [可选] 查看脚本版本号 ]
EOF
}

usage_and_exit () {
    usage
    exit "$1"
}

log () {
    echo "$@"
}

error () {
    echo "$@" 1>&2
    usage_and_exit 1
}

warning () {
    echo "$@" 1>&2
    EXITCODE=$((EXITCODE + 1))
}

version () {
    echo "$PROGRAM version $VERSION"
}

# 解析命令行参数，长短混合模式
(( $# == 0 )) && usage_and_exit 1
while (( $# > 0 )); do 
    case "$1" in
        -n | --virtualenv )
            shift
            VENV_NAME=$1
            ;;
        -w | --workon-home )
            shift
            WORKON_HOME=$1
            ;;
        -p | --python-path )
            shift
            PYTHON_PATH=$1
            ;;
        -a | --project-home )
            shift
            PROJECT_HOME=$1
            ;;
        -e | --encrypt )
            ENCRYPT=1
            ;;
        -r | --req-file )
            shift
            REQ_FILE=$1
            ;;
        -s | --pkg-path )
            shift
            PKG_PATH=$1
            ;;
        --help | -h | '-?' )
            usage_and_exit 0
            ;;
        --version | -v | -V )
            version 
            exit 0
            ;;
        -*)
            error "不可识别的参数: $1"
            ;;
        *) 
            break
            ;;
    esac
    shift $(( $# == 0 ? 0 : 1 ))
done 

# 参数合法性有效性校验，这些可以使用通用函数校验。
if [[ -z $VENV_NAME ]]; then
    warning "-n must be specify a valid name"
fi 
if ! [[ -r $REQ_FILE ]]; then
    warning "requirement file path does'nt exist"
fi
if ! [[ -x $PYTHON_PATH ]]; then
    warning "$PYTHON_PATH is not a valid executable python"
fi
if [[ -n $PROJECT_HOME ]] && [[ ! -d $PROJECT_HOME ]]; then
    warning "$PROJECT_HOME (-a, --project-home) specify a non-exist directory"
fi
if [[ $EXITCODE -ne 0 ]]; then
    exit "$EXITCODE"
fi

[[ -d "$WORKON_HOME" ]] || mkdir -p "$WORKON_HOME"
export WORKON_HOME

# pip固定死20.2.3，因为20.3.x 会存在解析依赖出错后就直接exit的问题
venv_opts=("-p" "$PYTHON_PATH" --no-download --no-periodic-update --pip 20.2.3)
if [[ -n "$PROJECT_HOME" ]]; then
    venv_opts+=("-a" "$PROJECT_HOME")
fi
if [[ -d $CTRL_DIR/pip ]]; then
    venv_opts+=(--extra-search-dir=$CTRL_DIR/pip)
fi

PYTHON_BIN_HOME=${PYTHON_PATH%/*}
export PATH=${PYTHON_BIN_HOME}:$PATH
if ! [[ -r ${PYTHON_BIN_HOME}/virtualenvwrapper.sh ]]; then
    # 写死固定版本号，以免出现高版本不兼容的情况
    local_options="--no-cache-dir --no-index --find-links $CTRL_DIR/pip"
    if [ -d $CTRL_DIR/pip ]; then
        "$PYTHON_BIN_HOME"/pip install ${local_options} pbr==5.5.1
        "$PYTHON_BIN_HOME"/pip install ${local_options} virtualenv==20.1.0 virtualenvwrapper==4.8.4
    else
        log "'$CTRL_DIR/pip' 目录不存在，线上安装virtualenvwrapper"
        "$PYTHON_BIN_HOME"/pip install pbr==5.5.1
        "$PYTHON_BIN_HOME"/pip install virtualenv==20.1.0 virtualenvwrapper==4.8.4
    fi
fi
# shellcheck source=/dev/null
export VIRTUALENVWRAPPER_PYTHON=${PYTHON_PATH}
source "${PYTHON_BIN_HOME}/virtualenvwrapper.sh"

# 加密解释器的特殊参数
if [[ "$ENCRYPT" -eq 1 ]]; then
    venv_opts+=(--system-site-packages --always-copy)
fi

set +e
if ! lsvirtualenv | grep -w "$VENV_NAME"; then
    if ! mkvirtualenv "${venv_opts[@]}" "$VENV_NAME"; then
        echo "create venv $VENV_NAME failed"
        exit 1
    fi
fi
set -e
if [[ -d "$PKG_PATH" ]];then
   PIP_OPTIONS+=("--find-links=$PKG_PATH" --no-index)
fi

"${WORKON_HOME}/$VENV_NAME/bin/pip" install -r "$REQ_FILE" "${PIP_OPTIONS[@]}"

if [[ $EXITCODE -ne 0 ]]; then
    warning "pip install error"
    exit "$EXITCODE"
fi
