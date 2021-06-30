#!/usr/bin/env bash

export LC_ALL=C LANG=C
SELF_PATH=$(readlink -f $0)
SELF_DIR=$(dirname $(readlink -f $0))
PKG_SRC_PATH=${SELF_DIR%/*}/src

. ${SELF_DIR}/functions

[[ -r $SELF_DIR/install.config ]] || fail "请先配置 install.config 文件"

check_yum_repo () {
   yum info nginx rabbitmq-server &>/dev/null
}

check_rabbitmq_version () {
   local mq_ver=$(yum list rabbitmq-server | grep -Eo '3\.[0-9]+\.[0-9]+')
   if [[ -n "$mq_ver" ]]; then
      return 0
   else
      echo "rabbitmq-server version below 3.0"
      return 1
   fi
}

generate_ip_array () {
   local ip_lines=$(awk '/^ *[0-9]/ { split($2,module,","); for (i=1; i<=length(module); i++) { print $1,module[i] } }' $SELF_DIR/install.config | sed 's/(.*)//' | sort -u)
   printf "export ALL_IP=(%s)\n" "$(awk '{print $1}' <<<"$ip_lines" | sort -u | xargs)"
   while read m; do
      awk -v module=$m 'BEGIN { printf "export %s_IP=(", toupper(module) }
      $2 == module { printf "%s ", $1}
      END { printf ")\n" } ' <<<"$ip_lines"
   done < <(awk '{print $2}' <<<"$ip_lines" | sort -u)
}

is_centos_7 () {
   which systemctl &>/dev/null
}

check_ssh_nopass () {
   for ip in ${ALL_IP[@]}; do
      echo -ne "$ip\t"
      ssh -o 'PreferredAuthentications=publickey' -o 'StrictHostKeyChecking=no' $ip "true" 2>/dev/null
      if [[ $? -eq 0 ]]; then
          echo "publickey Auth OK"
      else
          echo "publickey Auth FAILED, please configure no-pass login first."
          return 1
      fi
   done
   return 0
}

check_pip_config () {
   local url=$(awk '/^index-url/ { print $NF }'  ${PKG_SRC_PATH}/.pip/pip.conf)
   local code=$(curl -L -s -o /dev/null -w "%{http_code}" "$url")
   if [[ "$code" -eq 200 ]]; then 
       echo "pip config OK"
   else
       echo "check pip mirror in src/.pip/pip.conf "
       return 1
   fi
}

# firewalld
# NetworkManager
check_systemd_service () {
   local svc=$1
   if systemctl is-active --quiet $svc ; then
      echo "$svc is running, you should shutdown $svc"
      return 1
   else
      return 0
   fi
}

check_firewalld () {
   check_systemd_service "firewalld"
}

check_networkmanager () {
   check_systemd_service "NetworkManager"
}

check_selinux () {
   if [[ -x /usr/sbin/sestatus ]]; then
      if ! [[ $(/usr/sbin/sestatus -v | awk '/SELinux status/ { print $NF }') = "disabled" ]]; then
	 return 1
      fi
   fi
   return 0
}

check_umask () {
   if ! [[ $(umask) = "0022" ]]; then
      echo "umask shouled be 0022, now is <$(umask)>."
      return 1
   fi
}

check_open_files_limit () {
    if [[ $(ulimit -n) = "1024" ]];then
      echo "ulimit open files (-n)  should not be default 1024"
      echo "increase it up to 102400 or more for all BK hosts"
      return 1
    fi
}

# you can easily fix get_lan_ip () with following command:
# for example, your lan ip start with 9.x.x.x  
# grep -rl 'get_lan_ip *()' /data/install | xargs  sed -i 's#/\^10\\./#/^(10|9)\\./#' 
check_get_lan_ip () {
    local ip=$(get_lan_ip | head -1)
    if [[ -n "$ip" ]]; then
        return 0
    else
        echo "check \`ip addr\` output and get_lan_ip () in $CTRL_DIR/functions "
        return 1
    fi
}

check_password () {
   local INVALID=""
   source $SELF_DIR/globals.env 
   for v in MYSQL_PASS REDIS_PASS MQ_PASS ZK_PASS PAAS_ADMIN_PASS ZABBIX_ADMIN_PASS
   do
      eval pass=\$$v
      if [[ "$pass" =~ (\^|\?|%|&|\\|\/|\`|\!) ]]; then
          INVALID="$INVALID $v"
      fi
   done
   if echo "$INVALID" |grep -q "[A-Z]" 2>/dev/null; then
      echo "check $INVALID Variables in ${SELF_DIR}/globals.env"
      return 1
   else
      return 0
   fi

}

get_license_mac () {
   for ip in ${LICENSE_IP[@]}; do
      ssh $ip 'cat /sys/class/net/*/address'
   done
}

check_cert_mac () {
   local cert_file=${PKG_SRC_PATH}/cert/platform.cert
   if [[ ! -f "$cert_file" ]]; then
      echo "cert not exists"
      return 1
   fi
   local detail=$(openssl x509 -noout -text -in "$cert_file" 2>/dev/null)
   local cnt=$(grep -cFf <(get_license_mac) <(sed -n '/Subject Alternative Name:/{n;p}' <<<"$detail" | grep -Po '\b([a-z0-9]{2}:){5}[a-z0-9]{2}\b' ))
   [[ $cnt -eq ${#LICENSE_IP[@]} ]]
}

check_cert_passwd () {
   local passwd_file=${PKG_SRC_PATH}/cert/passwd.txt
   local env_file=${PKG_SRC_PATH}/$( <${PKG_SRC_PATH}/ENTERPRISE).env
   source $env_file

   local err_key=""
   # check gse key
   if ! [[ "$GSE_KEYTOOL_PASS" = "$(awk '/gse_job_api_client.p12/ { print $2 }' $passwd_file )" ]]; then
      err_key="GSE"
   fi
   if ! [[ "$JOB_KEYTOOL_PASS" = "$(awk '/job_server.p12/ { print $2 }' $passwd_file )" ]]; then
      err_key="$err_key JOB"
   fi

   if [[ "$err_key" =~ [A-Z] ]]; then
      echo "Following key pass is not matched: $err_key"
      return 1
   else
      return 0
   fi

}

check_opensrc_patch () {
   ( cd $PKG_SRC_PATH && \
      sed -e '/[B]EGIN_MD1/,/[E]ND_MD1/!d' $SELF_PATH \
      | sed '1d; $d' \
      | cut -b 2- \
      | md5sum --quiet -c -
   )

   if [[ $? -eq 0 ]]; then
      echo "check job-exec.war added LIBs"
      ( cd $PKG_SRC_PATH/job/job/ && \
	 unzip -l job-exec.war | grep -q mysql-connector-java 2>/dev/null
      )
   else
      echo "open source patch is not matched."
      return 1
   fi
}

check_http_proxy () {
   if [[ -n "$http_proxy" ]]; then
       echo "http_proxy variable is not empty."
       echo "you should use BK_PROXY in globals.env for http proxy when install blueking."
       return 1
   fi
}

check_domain () {
    local err_domain=""
    local err_fqdn=""
    source ${SELF_DIR}/globals.env

    # BK_DOMAIN 不能是顶级域名，没有\.字符时
    if ! [[ $BK_DOMAIN =~ \. ]]; then
        echo "globals.env中BK_DOMAIN不应该是顶级域名，请配置二级域名或者以上"
        return 1
    fi

    # FQDN等包含合法字符
    for d in BK_DOMAIN PAAS_FQDN JOB_FQDN CMDB_FQDN; do
        if ! [[ $(eval echo "\$$d") =~  ^[A-Za-z0-9.-]+\.[a-z]+$ ]]; then
            err_domain="$err_domain $d"
        fi
    done

    # FQDN 必须基于BK_DOMAIN
    for d in PAAS_FQDN JOB_FQDN CMDB_FQDN; do
        if ! [[ $(eval echo "\$$d") =~ $BK_DOMAIN$ ]]; then
            err_fqdn="$err_fqdn $d" 
        fi
    done

    if [[ -z "$err_domain" && -z "$err_fqdn" ]]; then
        return 0
    else
        [[ -n "$err_domain" ]] && echo "globals.env中以下域名包含非法字符：$err_domain"
        [[ -n "$err_fqdn" ]] && echo "globasl.env中以下FQDN没有以BK_DOMAIN结尾：$err_fqdn"
        return 1
    fi
}

check_service_dir () { 
    if ! [[ -d $PKG_SRC_PATH/service ]]; then
        echo "no service directory under $PKG_SRC_PATH. please extract it first."
        return 1
    fi
}

check_rsync () {
    if ! which rsync 2>/dev/null; then
        echo "please install <rsync> on all servers"
        echo "with `yum -y install rsync` command"
        return 1
    fi
    return 0
}

is_module_odd_num () {
   local module=$1
   local num=$(eval echo \${#${module}_IP[@]})
   if [[ $num -gt 0 && $((num%2)) -eq 0 ]]; then
      echo "$module 模块(当前值$num)在install.config中数量为偶数，而不是奇数。"
      return 1
   fi
}

check_install_config () {
   # gse redis on same host
   if ! [[ "$GSE_IP0" = "$REDIS_IP0" ]]; then
      echo "gse 和 redis 必须部署在同一台机器上。"
      return 1
   fi
   # 检查是否有不合法的行
   local error_line=( $(awk '/^[0-9]/ && NF != 2 { print NR }' $SELF_DIR/install.config) )
   if [[ ${#error_line[@]} -ge 1 ]]; then
      echo "install.config中第$(printf "%s," ${error_line[@]})行的IP和模块之间没有用空白分隔。"
      return 1
   fi
   local ret=0
   for m in CONSUL KAFKA ZK ; do
      is_module_odd_num $m || ((++ret))
   done
   return $ret
}

check_glibc_version () {
    local base_version=2.14
    if ! command -v rpmdev-vercmp &>/dev/null; then
        yum -q -y install rpmdevtools
    fi
    rpmdev-vercmp "$base_version" "$(rpm -q --queryformat "%{VERSION}" glibc-common)"
    case $? in
        0 | 12) return 0 ;;
        11 ) echo "glibc的版本低于2.14，不符合部署gse的最低要求" ;;
        * ) echo "未知错误" ;;
    esac
    return 1
}

do_check() {
   local item=$1
   local step_file=$HOME/.bk_precheck

   if grep -qw "$item" $step_file; then
        echo "<<$item>> has been checked successfully... SKIP"
   else
        echo -n "start <<$item>> ... "
        message=$($item)
        if [ $? -eq 0 ]; then
            echo "[OK]"
            echo "$item" >> $step_file
        else
            echo "[FAILED]"
            echo -e "\t$message"
            exit 1
        fi
   fi
}

if [[ -z $BK_PRECHECK ]]; then
    BK_PRECHECK="check_ssh_nopass check_password check_cert_mac check_get_lan_ip
    check_install_config check_selinux check_umask check_rabbitmq_version
    check_http_proxy check_open_files_limit check_domain check_rsync check_service_dir
    check_glibc_version
    check_cert_passwd check_opensrc_patch
    "
fi

if [[ -z "$BK_OPTIONAL_CHECK" ]]; then
    BK_OPTIONAL_CHECK="check_networkmanager check_firewalld"
fi

STEP_FILE=$HOME/.bk_precheck

# 根据参数设置标记文件
if [ "$1" = "-r" -o "$1" = "--rerun" ]; then
    > "$STEP_FILE"
else
   [ -e "$STEP_FILE" ] || touch $STEP_FILE 
fi

eval "$(generate_ip_array)"
for item in $BK_PRECHECK
do
   do_check $item
done

if is_centos_7 ; then
   for c in $BK_OPTIONAL_CHECK
   do
      do_check $c
   done
fi

#BEGIN_MD1
#8064835579ddb2c86ae26b447b0c5f76  ./job/job/WEB-INF/lib/logback-classic-1.1.11.jar
#8362f161170fa58b2497aa26bcc9081b  ./job/job/WEB-INF/lib/jboss-marshalling-1.3.0.CR9.jar
#90c63f0e53e6f714dbc7641e066620e4  ./job/job/WEB-INF/lib/jchardet-1.0.jar
#decf9c8c6d376b0740fad815c97ffcc0  ./job/job/WEB-INF/lib/amqp-client-4.0.3.jar
#4f4496e12763a3d5e78e9740bae8b91f  ./job/job/WEB-INF/lib/jboss-marshalling-serial-1.3.0.CR9.jar
#640c58226e7bb6beacc8ac3f6bb533d1  ./job/job/WEB-INF/lib/c3p0-0.9.1.1.jar
#4c73735809d11c934772b1128aec169f  ./job/job/WEB-INF/lib/logback-access-1.1.11.jar
#79de69e9f5ed8c7fcb8342585732bbf7  ./job/job/WEB-INF/lib/javax.servlet-api-3.1.0.jar
#33e190a0f0745306de54fba90f381fc3  ./job/job/WEB-INF/lib/ecj-3.12.3.jar
#0c4f91d3adc5de7fac8806eb97308554  ./job/job/WEB-INF/lib/mysql-connector-java-5.1.39.jar
#cc7a8deacd26b0aa2668779ce2721c0f  ./job/job/WEB-INF/lib/logback-core-1.1.11.jar
#654f75b302db6ed8dc5a898c625e030c  ./bkdata/support-files/pkgs/MySQL-python-1.2.5.zip
#25107741bc7f4889a3a0f5f13b8c06de  ./bkdata/support-files/pkgs/uwsgi-2.0.18.tar.gz
#6b54c704609b42a242d7d018e9f1bd16  ./bkdata/databus/lib/common/waffle-jna-1.7.5.jar
#8f497455cbe1b6707d7e664c4f4be484  ./bkdata/databus/lib/common/mysql-connector-java-5.1.38.jar
#51050e595b308c4aec8ac314f66e18bc  ./bkdata/databus/lib/common/xz-1.5.jar
#4021551de5018dfa4b79ec553280f00a  ./bkdata/databus/lib/common/logback-core-1.1.7.jar
#END_MD1
