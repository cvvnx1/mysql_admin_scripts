#!/bin/bash

# You need comfirm these vars
exec_mysqldump=/usr/bin/mysqldump
exec_mysql=/usr/bin/mysql
exec_mycnf=/usr/bin/my_print_defaults
db_socket=/var/lib/mysql/mysql.sock
db_host=
db_user=
db_psw=
db_name=test
opt=
binlog_dir=/opt/mysql
back_dir=/opt/dbback

# Do not modify db_connect
db_connect=

mysql_connect_str(){
  if [ "${db_socket}" = "" ]; then
    db_connect="-h${db_host} -u${db_user} -p${db_psw}"
  else
    db_connect="-S${db_socket}"
  fi
}

full_backup(){
  local run_dir=$(pwd)
  [ ! -d ${back_dir} ] && mkdir -pv ${back_dir}
  local nowdate=$(date "+%Y%m%d%H%M%S")
  mysql_connect_str
  [ -f ${back_dir}/full_${db_name}_${nowdate}.sql ] && echo "Backup file exist: ${back_dir}/full_${db_name}_${nowdate}.sql" && return 1
  ${exec_mysqldump} ${db_connect} --flush-logs --master-data=2 ${opt} ${db_name} > ${back_dir}/full_${db_name}_${nowdate}.sql
echo "[$(date "+%x:%X %z")] Dump to ${back_dir}/full_${db_name}_${nowdate}.sql"
  local binlog_last=$(cat ${back_dir}/full_${db_name}_${nowdate}.sql | grep MASTER_LOG_FILE | awk -F"'" '{print $2}')
echo "[$(date "+%x:%X %z")] Next binlog is ${binlog_last}"
  ${exec_mysql} ${db_connect} -e "PURGE BINARY LOGS TO '${binlog_last}'"
echo "[$(date "+%x:%X %z")] Purge binlog to ${binlog_last}"
  cd ${back_dir}
  tar zcf full_${db_name}_${nowdate}.tar.gz full_${db_name}_${nowdate}.sql
  rm -f full_${db_name}_${nowdate}.sql && 
echo "[$(date "+%x:%X %z")] Tar to full_${db_name}_${nowdate}.tar.gz"
  cd ${run_dir}
  return 0
}

increment_backup(){
  local run_dir=$(pwd)
  [ ! -d ${back_dir} ] && mkdir -pv ${back_dir}
  local nowdate=$(date "+%Y%m%d%H%M%S")
  mysql_connect_str
  local binlog_prefix=$(${exec_mysql} -N -e "SHOW BINARY LOGS" | head -n1 | awk -F"." '{print $1}')
  local backup_files=$(${exec_mysql} ${db_connect} -e "SHOW BINARY LOGS" | grep ${binlog_prefix} | awk '{print $1}')
  local file_list=""
  ${exec_mysql} ${db_connect} -e "FLUSH LOGS"
  for FILE in $backup_files
  do
    [ -f ${back_dir}/$FILE ] && echo "Backup file exist: ${back_dir}/$FILE" && return 1
    [ -f ${binlog_dir}/$FILE ] && mv ${binlog_dir}/$FILE ${back_dir} && file_list="$FILE ${file_list}" && 
echo "[$(date "+%x:%X %z")] Backup ${binlog_dir}/$FILE"
    binlog_last=$FILE
  done
echo "[$(date "+%x:%X %z")] Next binlog is ${binlog_last}"
  ${exec_mysql} ${db_connect} -e "PURGE BINARY LOGS TO '${binlog_last}'"
echo "[$(date "+%x:%X %z")] Purge binlog to ${binlog_last}"
  cd ${back_dir}
  tar zcf incre_${db_name}_${nowdate}.tar.gz ${file_list}
  rm -f ${back_dir}/${file_list} && 
echo "[$(date "+%x:%X %z")] Tar to incre_${db_name}_${nowdate}.tar.gz"
  cd ${run_dir}

  return 0
}

status(){
  mysql_connect_str
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
    echo $"Usage: $0 {full|increment|status|help}"
    RETVAL=2
esac