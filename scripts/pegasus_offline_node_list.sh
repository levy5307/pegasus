#!/bin/bash
#
# Offline replica servers using minos.
#

PID=$$

if [ $# -le 2 ]; then
  echo "USAGE: $0 <cluster-name> <cluster-meta-list> <replica-task-id-list>"
  echo
  echo "For example:"
  echo "  $0 onebox 127.0.0.1:34601,127.0.0.1:34602 1,2,3"
  echo
  exit 1
fi

cluster=$1
meta_list=$2
replica_task_id_list=$3

pwd="$( cd "$( dirname "$0"  )" && pwd )"
shell_dir="$( cd $pwd/.. && pwd )"
cd $shell_dir

echo "Check the argument..."
source ./scripts/pegasus_check_arguments.sh offline_node_list $cluster $meta_list $replica_task_id_list

if [ $? -ne 0 ]; then
    echo "ERROR: the argument check failed"
    exit 1
fi

echo "Set lb.assign_secondary_black_list..."
echo "remote_command -l $pmeta meta.lb.assign_secondary_black_list $address_list" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.offline_node_list.assign_secondary_black_list
set_ok=`grep "set ok" /tmp/$UID.$PID.pegasus.offline_node_list.assign_secondary_black_list | wc -l`
if [ $set_ok -ne 1 ]; then
  echo "ERROR: set lb.assign_secondary_black_list failed, refer to /tmp/$UID.$PID.pegasus.offline_node_list.assign_secondary_black_list"
  exit 1
fi

echo "Set live_percentage to 0...(No need to set it back to default. When this offline task is done, meta-server should be restarted, and live_percentage will be loaded from the config file)"
echo "remote_command -l $pmeta meta.live_percentage 0" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.offline_node.live_percentage
set_ok=`grep OK /tmp/$UID.$PID.pegasus.offline_node.live_percentage | wc -l`
if [ $set_ok -ne 1 ]; then
  echo "ERROR: set live_percentage to 0 failed"
  exit 1
fi

echo
for id in $id_list
do
  echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
  ./scripts/pegasus_offline_node.sh $cluster $meta_list $id
  if [ $? -ne 0 ]; then
    echo "ERROR: offline replica task $id failed"
    exit 1
  fi
  echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
  echo "sleep for 10 seconds"
  sleep 10
done

echo "Clear lb.assign_secondary_black_list..."
echo "remote_command -l $pmeta meta.lb.assign_secondary_black_list clear" | ./run.sh shell --cluster $meta_list &>/tmp/$UID.$PID.pegasus.offline_node_list.assign_secondary_black_list
set_ok=`grep "clear ok" /tmp/$UID.$PID.pegasus.offline_node_list.assign_secondary_black_list | wc -l`
if [ $set_ok -ne 1 ]; then
  echo "ERROR: clear lb.assign_secondary_black_list failed, refer to /tmp/$UID.$PID.pegasus.offline_node_list.assign_secondary_black_list"
  exit 1
fi

all_finish_time=$((`date +%s`))
echo "Offline replica server task list done."
echo "Elapsed time is $((all_finish_time - all_start_time)) seconds."

rm -f /tmp/$UID.$PID.pegasus.* &>/dev/null
