#!/usr/bin/env bash

SELF_DIR=$(readlink -f "$(dirname "$0")")

[[ -f "$SELF_DIR"/../utils.fc ]] && . "$SELF_DIR"/../utils.fc 

JQ=${SELF_DIR}/../bin/jq
# 检查曾经出现过的jwt的wlist字段含有非法字符的问题
check_esb_jwt_wlist () {
    local wlist
    wlist=$(MYSQL_PWD="$MYSQL_PASS" mysql -u"$MYSQL_USER" -h "$MYSQL_IP0" -NBe \
    "use open_paas; select wlist from esb_function_controller where func_code = 'jwt::private_public_key';")
    if ! $JQ -r <<<"$wlist" &>/dev/null; then
        echo "esb的function controller表中存在脏数据"
        echo "使用管理员身份打开 $HTTP_SCHEMA://$PAAS_FQDN/admin/bkcore/functioncontroller/"
        echo "点击进入jwt::private_public_key, 删除功能测试白名单列右侧内容中的非法字符，使得它变成合法的json"
        return 1
    fi 
    return 0
}