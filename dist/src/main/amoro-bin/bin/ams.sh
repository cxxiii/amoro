#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# 获取当前脚本所在目录
CURRENT_DIR="$( cd "$(dirname "$0")" ; pwd -P )"

# 加载配置文件
source ${CURRENT_DIR}/load-config.sh

# 设置Java虚拟机参数
JAVA_OPTS="-server -XX:+UseG1GC -XX:MaxGCPauseMillis=200 \
-Xloggc:$AMORO_LOG_DIR/gc.log -XX:+PrintGCDateStamps -XX:+IgnoreUnrecognizedVMOptions -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=10M \
-Xms${JVM_XMS_CONFIG}m -Xmx${JVM_XMX_CONFIG}m \
-verbose:gc -XX:+PrintGCDetails \
--add-opens=java.base/java.lang=ALL-UNNAMED \
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
--add-opens=java.base/java.io=ALL-UNNAMED \
--add-opens=java.base/java.net=ALL-UNNAMED \
--add-opens=java.base/java.nio=ALL-UNNAMED \
--add-opens=java.base/java.util=ALL-UNNAMED \
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
--add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
--add-opens=java.base/sun.security.action=ALL-UNNAMED \
--add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
"

# 如果配置了JMX远程端口，添加JMX相关参数
if [ -n "$JMX_REMOTE_PORT_CONFIG" ];then
  JAVA_OPTS="${JAVA_OPTS} -Dcom.sun.management.jmxremote.port=${JMX_REMOTE_PORT_CONFIG} \
  -Dcom.sun.management.jmxremote \
  -Dcom.sun.management.jmxremote.ssl=false \
  -Dcom.sun.management.jmxremote.authenticate=false \
  "
fi

# 如果配置了额外的JVM参数，追加到JAVA_OPTS中
if [ ! -z "$JVM_EXTRA_CONFIG" ];then
    JAVA_OPTS="${JAVA_OPTS} ${JVM_EXTRA_CONFIG}"
fi

# 设置要运行的主类
RUN_SERVER="org.apache.amoro.server.AmoroServiceContainer"

# 定义路径变量
LIB_PATH=$AMORO_HOME/lib
STDERR_LOG=${AMORO_LOG_DIR}/app.log.err
PID=${AMORO_HOME}/run/app.pid

# 创建日志目录
if [ ! -d "$AMORO_LOG_DIR" ]; then
    mkdir "$AMORO_LOG_DIR"
fi

# 创建运行目录
if [ ! -d "${AMORO_HOME}/run" ]; then
    mkdir "${AMORO_HOME}/run"
fi

# 创建PID文件
if [ ! -f $PID_PATH ];then
    touch $PID_PATH
fi

# 创建错误日志文件
if [ ! -f $STDERR_LOG ];then
    touch $STDERR_LOG
fi

# 设置默认JVM参数
if [ -z "$JAVA_OPTS" ]; then
    JAVA_OPTS="-Xms512m -Xmx512m -verbose:gc -XX:+PrintGCDetails"
fi

# 设置类路径
export CLASSPATH=$AMORO_CONF_DIR:$LIB_PATH/:$(find $LIB_PATH/ -type f -name "*.jar" | sort | paste -sd':' -)

# 如果有额外的类路径，追加到CLASSPATH中
if [ -n "${AMORO_ADDITION_CLASSPATH}" ]; then
    export CLASSPATH=$AMORO_ADDITION_CLASSPATH:$CLASSPATH
fi

# 构建完整的启动命令
CMDS="$JAVA_RUN -Dlog4j.configurationFile=${AMORO_LOG_CONF_FILE} -Dlog.home=${AMORO_LOG_DIR} -Dlog.dir=${AMORO_LOG_DIR} -Duser.dir=${AMORO_HOME}  $JAVA_OPTS ${RUN_SERVER}"

# 检查服务状态函数
# 返回值说明：
# 0: PID文件不存在但进程运行正常
# 1: PID文件存在但进程已停止
# 2: PID文件不存在
function status(){
    test -e ${PID} || return 2
    test -n "$(ps -p $(cat ${PID}) -o pid=)" && return 0 || return 1
}

# 启动服务函数
function start() {
  nohup ${CMDS} >/dev/null 2>>${STDERR_LOG} &
    if [ $? -ne 0 ]; then
        echo "start failed."
    fi
    echo $! > ${PID}; sleep 1.5
    if status ; then
        echo "process start success."; return 0
    else
        echo "process start failed."; return 1
    fi
}

# 前台启动服务函数
function startForeground() {
  exec ${CMDS}
}

# 停止服务函数
function stop() {
    status && kill $(cat ${PID})
    if ! status; then
        rm -f ${PID};
        echo "stop success."; return 0
    fi
    
    kill_times=0
    while status
    do
        sleep 1
        let kill_times++
        if [ ${kill_times} -eq 10 ]
        then
            kill -9 $(cat ${PID})
            sleep 3; break
        fi
    done

    if status; then
        echo "stop failed. process is still running."; return 1
    else
        rm -f ${PID};
        echo "stop success."; return 0
    fi
}

# 处理命令行参数
case "$1" in
    start)
        status;
        status_return=$?;
        if [ $status_return -eq 2 ]; then
            echo 'starting app server.'
            start
        elif [ $status_return -eq 0 ]; then
            echo "alreadly running. start app failed." 
        else 
            echo "the pid file exists but porc is down; will delete ths pidfile ${PID} and starting app server."
            start
        fi
        ;;
    start-foreground)
       startForeground
       ;;
    stop)
        status;
        if [ $? -ne 0 ]; then
            echo "proc not running."
        else
            echo 'stopping app server.'
            stop
        fi
        ;;
    restart)
        stop && sleep 3 && start
        ;;
    status)
        status;
        if [ $? -eq 0 ];then
            echo 'running.'
        else
            echo 'not running.'
            exit 1
        fi
        ;;
    pid)
        status
        if [ $? -eq 0 ]; then
          cat $PID
        else
          echo "not running"
        fi
        ;;
    *)
        echo "Usage $0 start|start-foreground|stop|restart|status|pid"
        exit 1
        ;;
esac
