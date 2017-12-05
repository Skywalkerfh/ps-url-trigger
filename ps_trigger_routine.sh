#!/bin/bash

# ---------------------------------------------------------
# @Filename     : ps_trigger_routine.sh
# @Date         : 2017/11/11 12:30:31
# @Author       :
# @Description  :
# ---------------------------------------------------------
# Copyright (c) 2017 Baidu.com, Inc. All Rights Reserved
# ---------------------------------------------------------

declare current_path=`pwd`
source /home/work/fenghao01/bt/utils/bt-basic.conf
source /home/work/fenghao01/bt/routine/trigger/conf/task.cfg

function gen_ps_input()
{
    local countwindow=1
    local hadoopbin=${mulan_hadoopbin}
    local ps_wise_rootPath=${khan_hdfs}${ps_wise_rootPath}
    local ps_pc_rootPath=${khan_hdfs}${ps_pc_rootPath}

    local pc_day_count=0
    local num=0
    pc_inputfile=$ps_pc_rootPath'{'
    while [[ $pc_day_count -lt ${countwindow} ]]; do
        local date=`date -d -${num}day +%Y%m%d`
        ps_pc_path=${ps_pc_rootPath}${date}'/to.hadoop.done'
        ((num ++));
        ${hadoopbin} fs -test -e ${ps_pc_path}
        if [ $? -ne 0 ]; then
            echo "${ps_pc_path} do not exist!"
        else
            ((pc_day_count++))
            pc_inputfile=${pc_inputfile}${date}','
        fi

        if [ $num -gt 30 ]; then
            echo "pc hdfs log searching days larger than 30!"
            exit -1
        fi
    done;
    pc_inputfile=${pc_inputfile%,*}'}'

    local wise_day_count=0
    num=0
    wise_inputfile=$ps_wise_rootPath'{'
    while [[ $wise_day_count -lt ${countwindow} ]]; do
        date=`date -d -${num}day +%Y%m%d`
        ps_wise_path=${ps_wise_rootPath}${date}
        ((num ++));
        ${hadoopbin} fs -test -e ${ps_wise_path}'/to.hadoop.done'
        if [ $? -ne 0 ]; then
            echo "${ps_wise_path} do not exist!"
        else
            ((wise_day_count++));
            wise_inputfile=${wise_inputfile}${date}','
        fi

        if [ ${num} -gt 30 ]; then
            echo "wise hdfs log searching days larger than 30!!"
            exit -1
        fi
    done;
    wise_inputfile=${wise_inputfile%,*}'}'
}

gen_ps_input
date_yesterday=`date -d -1day +%Y%m%d`
date=`date -d -0day +%Y%m%d`

hadoopbin=${mulan_hadoopbin}
script=ps_trigger_script_online.py
inputfile=${pc_inputfile}' -input '${wise_inputfile}
outputfile=${mulan_hdfs}${mulan_output_dir}${date}

partition_num=1
key_fields_sort=4
jobname="bt-ps-trigger-routine"
${hadoopbin} fs -rmr ${outputfile}
${hadoopbin} streaming \
    -D mapred.job.priority=VERY_HIGH \
    -D mapred.reduce.tasks=50 \
    -D stream.memory.limit=4096 \
    -D mapred.job.name=${jobname} \
    -D mapred.job.map.capacity=3000 \
    -cacheArchive /share/python2.7.tar.gz#python2.7 \
    -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
    -jobconf stream.num.map.output.key.fields=${key_fields_sort} \
    -jobconf num.key.fields.for.partition=${partition_num} \
    -file ${script} \
    -file ${pkg_show_url} \
    -file ${pkg_show_url_blacklist} \
    -file ${blue_words} \
    -mapper "python2.7/bin/python ${script} map" \
    -reducer "python2.7/bin/python ${script} red" \
    -input ${inputfile}\* \
    -output ${outputfile} \

PY=${python}
tmp_file=${savedata_path}${date}_tmp
app_info=${appname_info}
${hadoopbin} fs -getmerge ${outputfile} ${tmp_file}
${PY} rig_input_format.py ${app_info} ${tmp_file} > ${rig_path}/rel_input

rig_path=${rig_path}
cd ${rig_path}
sh -x run_predict_colorful.sh rel_input fenghao01_ps_url_rig${date} shitu_base fea_refine2 predict 1 model_refine2 > ${log_path}/rel_input.${date}.model_refine2 2>&1 &

cd ${current_path}
#rigq 阈值过滤
#竞品词过滤
#现有pair过滤
