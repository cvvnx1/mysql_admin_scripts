#!/bin/bash

# You need comfirm these vars
exec_mysqldump=/usr/local/mysql/bin/mysqldump
exec_mysql=/usr/local/mysql/bin/mysql
exec_mycnf=/usr/local/mysql/bin/my_print_defaults
db_socket=
db_host=
db_user=
db_psw=
db_name=test
opt=
binlog_dir=/opt/mysql/dbinstance
back_dir=/opt/dbback

# Do not modify db_connect
db_connect=

mysql_connect_str(){
  if [ "${db_socket}" = "" ]; then
    db_connect="-h${db_host} -u${db_user} -p${db_psw}"
  else
    db_connect="-s${db_socket}"
  fi
}

full_backup(){
  local run_dir=$(pwd)
  [ ! -d ${back_dir} ] && mkdir -pv ${back_dir}
  local nowdate=$(date "+%Y%m%d%H%M%S")
  [ -f ${back_dir}/full_${db_name}_${nowdate}.sql ] && echo "Backup file exist: ${back_dir}/full_${db_name}_${nowdate}.sql" && return 1
  ${exec_mysqldump} ${db_connect} --flush-logs --master-data=2 ${opt} ${db_name} > ${back_dir}/full_${db_name}_${nowdate}.sql
  local binlog_point=$(cat ${back_dir}/full_${db_name}_${nowdate}.sql | grep MASTER_LOG_FILE | awk -F"'" '{print $2}')
  ${exec_mysql} ${db_connect} -e "PURGE BINARY LOGS TO '${binlog_point}'"
  cd ${back_dir}
  tar zcf full_${db_name}_${nowdate}.tar.gz full_${db_name}_${nowdate}.sql
  rm -f full_${db_name}_${nowdate}.sql
  cd ${run_dir}
  return 0
}

increment_backup(){
  local run_dir=$(pwd)
  [ ! -d ${back_dir} ] && mkdir -pv ${back_dir}
  local nowdate=$(date "+%Y%m%d%H%M%S")
  local binlog_prefix=$(${exec_mycnf} mysqld | grep log-bin | sed "s/^.*=//")
  local backup_files=$(${exec_mysql} ${db_connect} -e "SHOW BINARY LOGS" | grep ${binlog_prefix} | awk '{print $1}')
  ${exec_mysql} ${db_connect} -e "FLUSH LOGS"
  for FILE in $backup_files
  do
    [ -f ${back_dir}/$FILE ] && echo "Backup file exist: ${back_dir}/$FILE" && return 1
    mv ${binlog_dir}/$FILE ${back_dir}
    binlog_last=$FILE
  done
  cd ${back_dir}
  tar zcf incre_${db_name}_${nowdate}.tar.gz ${backup_files}
  rm -f ${back_dir}/${backup_files}
  cd ${run_dir}
  ${exec_mysql} ${db_connect} -e "PURGE BINARY LOGS TO '${binlog_last}'"
  return 0
}

status(){
  local binlog_prefix=$(${exec_mycnf} mysqld | grep log-bin | sed "s/^.*=//")
  local backup_files=$(${exec_mysql} ${db_connect} -e "SHOW BINARY LOGS" | grep ${binlog_prefix} | awk '{print $1}')
  for FILE in $backup_files
  do
    binlog_last=$FILE
  done
  echo "Binary log point: ${binlog_last}"
  return 0
}

# See how we were called.
case "$1" in
  full)
    full_backup
    RETVAL=$?
    ;;
  increment)
    increment_backup
    RETVAL=$?
    ;;
  status)
    status
    RETVAL=$?
    ;;
  *)
    echo $"Usage: $prog {full|increment|status|help}"
    RETVAL=2
esac
