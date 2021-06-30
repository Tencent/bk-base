#!/bin/bash
# Desc: uninstall blueking suit on single host

export LC_ALL=C LANG=C
SELF_PATH=$(readlink -f $0)
SELF_DIR=$(dirname $(readlink -f $0))

CTRL_DIR=${SELF_DIR}
PKG_SRC_PATH=${CTRL_DIR%/*}/src
INSTALL_PATH=$(< $CTRL_DIR/.path)
INSTALL_PATH=$(readlink -f $INSTALL_PATH) # in case it is a symlink
BK_DOMAIN=$(awk -F'[\"=]' '/^export BK_DOMAIN/ { print $3 }' $CTRL_DIR/globals.env ) 

# load common functions
if [[ -r $CTRL_DIR/functions ]]; then
    source $CTRL_DIR/functions
else
    echo "you should cp uninstall.sh to /data/install, not directly invoked under $SELF_DIR/"
    exit 1
fi

# include some useful functions
clean_crontab () {
    log "clean crontab settings"
    crontab <( crontab -l | grep -v "/watch.rc;" \
                          | grep -v "/process_watch " \
                          | grep -v "$INSTALL_PATH" \
                          | grep -v "/usr/local/gse/" )
    return 0
}

# check user
[[ $USER = "root" ]] || fail "please run $0 as root."

# confirm
echo "this script will kill all processes related to Blueking Suit on $LAN_IP"
echo "and following directories, make sure you have backups before confirm"
echo "- $PKG_SRC_PATH"
echo "- $INSTALL_PATH"
echo "- $CTRL_DIR"
echo "if you really want to uninstall, please input yes to continue"
read -p "yes/no? " reply
if [[ "$reply" != "yes" ]]; then
    echo "Abort"
    exit 1
fi

step "clear crontab entry contains $INSTALL_PATH"
clean_crontab

# if docker running, stop all,delete all,first
if mount | grep -q public/paas_agent/docker 2>/dev/null; then
    docker kill $(docker ps -q)
    docker rm $(docker ps -a -q)
    docker rmi $(docker images -q)
    pkill -9 dockerd containerd
    mount_point=$(mount  | grep -Eo "$INSTALL_PATH/public/paas_agent/docker/[a-z]+")
    [[ $mount_point =~ docker ]] && umount $mount_point
fi

# if docker start by systemd
systemctl stop docker

# STOP all process running under $INSTALL_PATH
step "kill all process running under $INSTALL_PATH"
if [[ -e "$INSTALL_PATH" && "$INSTALL_PATH" != "/" ]]; then
    stat -c %N /proc/*/fd/* 2>&1 \
        | awk -v p="$INSTALL_PATH" '$0 ~ p {print $1}' \
        | awk -F/ '{print $3}' | sort -u | xargs kill -9 
else
    echo "$INSTALL_PATH is not exist or equal \"/\""
    exit 1
fi

# STOP all processes left
pkill -9 gunicorn
pkill -9 influxd 

# uninstall bk python rpm
step "remove blueking python rpm installed under /opt/py27 and /usr/local/bin"

log "remove /opt/py27 python"
rpm -ev python27-2.7.9 python27-devel

log "remove /usr/local/bin/* python related files"
rm -fv /usr/local/bin/easy_install /usr/local/bin/pip \
/usr/local/bin/supervisord /usr/local/bin/easy_install-2.7 \
/usr/local/bin/pip2 /usr/local/bin/virtualenv \
/usr/local/bin/echo_supervisord_conf  /usr/local/bin/pip2.7 \
/usr/local/bin/virtualenv-clone /usr/local/bin/pbr \
/usr/local/bin/python /usr/local/bin/virtualenvwrapper_lazy.sh \
/usr/local/bin/pidproxy /usr/local/bin/supervisorctl \
/usr/local/bin/virtualenvwrapper.sh

# uninstall rpm installed 
step "remove rabbitmq nginx beanstalkd"
systemctl stop rabbitmq-server
service rabbitmq-server stop 
pkill -9 nginx
pkill -9 beanstalkd
pkill -9 redis-server 
pkill -9 influxd
yum -y remove rabbitmq-server beanstalkd nginx influxdb

# uninstall nfs service if exists
if mount | grep -q "$INSTALL_PATH"/public/nfs ; then
    umount -f -l $INSTALL_PATH/open_paas/paas/media
    umount -f -l $INSTALL_PATH/public/job
fi

if ps -C nfsd &>/dev/null; then
    step "remove nfs service"
    service nfs stop 
    service rpcbind stop 
    sed -i "/public\/nfs/d" /etc/exports
    chkconfig --level 345 nfs off
    chkconfig --level 345 rpcbind off
fi

step "remove /etc/hosts entry; remove $HOME/.bkrc; remove /etc/resolv.conf entry; remove other left over files"
sed -i "/$BK_DOMAIN/d" /etc/hosts
sed -i '/127.0.0.1/d' /etc/resolv.conf
rm -fv $HOME/.bkrc $HOME/.precheck $HOME/.erlang.cookie /etc/rc.d/bkrc.local $HOME/.bk_precheck
rm -rf $HOME/.virtualenvs 
rm -rf /var/lib/rabbitmq
rm -rf /var/lib/nginx

if ps -C agentWorker &>/dev/null; then
    step "remove gse_agent"
    for p in gseMaster agentWorker basereport bkmetricbeat processbeat unifytlogc; do 
        pkill -9 $p 
    done
    rm -rf /usr/local/gse /var/log/gse /var/run/gse /var/lib/gse
fi


step "remove $INSTALL_PATH $PKG_SRC_PATH $CTRL_DIR"
chattr -i $CTRL_DIR/.migrate/* 2>/dev/null
[[ -e $PKG_SRC_PATH ]] && rm -rf ${PKG_SRC_PATH}
[[ -e $INSTALL_PATH ]] && rm -rf ${INSTALL_PATH}
[[ -e $CTRL_DIR ]] && rm -rf ${CTRL_DIR}

step "Run(lsof +L1) to see if any process exists"
if lsof +L1 | grep -E "$INSTALL_PATH|/var/log/gse"; then
    echo "if you really want to kill these left over processes, please input yes to continue"
    read -p "yes/no? " reply
    if [[ "$reply" != "yes" ]]; then
        echo "Abort"
        exit 1
    else
    lsof +L1 | grep -E "$INSTALL_PATH|/var/log/gse" \
        | awk '$1 != "lsof" { print $2 }' | sort -u \
        | xargs kill -9 
    fi
fi

# 清理用户
for u in nginx rabbitmq epmd es cmdb apps beanstalkd influxdb etcd; do
    userdel $u 2>/dev/null
done