import os
import json
from collections import OrderedDict

def processMR():
    cwd = os.getcwd()
    os.chdir('mr_history')
    queries = os.listdir(os.getcwd())
    for query in queries:
        print query

        total_tasks = 0
        total_map = 0
        total_reduce = 0
        tasks_time = OrderedDict()

        ori_path = os.getcwd()
        os.chdir(query)
        jobs_file = os.listdir(os.getcwd())
        for job_file in jobs_file:
            if job_file != query:
                print job_file
                with open(job_file) as job:
                    lines = job.readlines()
                    for line in lines[1:]:
                        if line != '\n':
                            entity = json.loads(line)
                            if 'type' in entity and entity['type'] == 'JOB_INITED':
                                event = entity['event']
                                entry = event['org.apache.hadoop.mapreduce.jobhistory.JobInited']
                                if 'totalMaps' in entry:
                                    total_maps = entry['totalMaps']
                                    total_tasks += total_maps
                                    total_map += total_maps
                                if 'totalReduces' in entry:
                                    total_reduces = entry['totalReduces']
                                    total_tasks += total_reduces
                                    total_reduce += total_reduces
                            if 'type' in entity and entity['type'] == 'TASK_STARTED':
                                task = OrderedDict()
                                event = entity['event']
                                entry = event['org.apache.hadoop.mapreduce.jobhistory.TaskStarted']
                                taskid = entry['taskid']
                                task['taskType'] = entry['taskType']
                                task['startTime'] = entry['startTime']
                                tasks_time[taskid] = task
                            if 'type' in entity and entity['type'] == 'TASK_FINISHED':
                                event = entity['event']
                                entry = event['org.apache.hadoop.mapreduce.jobhistory.TaskFinished']
                                task = tasks_time[entry['taskid']]
                                task['finishTime'] = entry['finishTime']
                                task['status'] = entry['status']
                                print task
        print total_tasks
        print len(tasks_time) == total_tasks

        with open(query, 'w') as write_file:
            write_file.write('total_tasks: '+str(total_tasks)+'\n')
            write_file.write('total_mappers: '+str(total_map)+'\n')
            write_file.write('total_reducers: '+str(total_reduce)+'\n')
            for id in tasks_time:
                write_file.write(id+' ')
                item = tasks_time[id]
                for key in item:
                    write_file.write(key + ':' + str(item[key]) + ' ')
                write_file.write('\n')

        os.chdir(ori_path)
    os.chdir(cwd)

def processTez():
    cwd = os.getcwd()
    os.chdir('tez_history')
    queries = os.listdir(os.getcwd())
    for query in queries:
        print query

        total_tasks = 0
        tasks_time = OrderedDict()

        ori_path = os.getcwd()
        os.chdir(query)
        jobs_file = os.listdir(os.getcwd())
        for job_file in jobs_file:
            if job_file != query:
                print job_file
                with open(job_file) as job:
                    lines = job.readlines()
                    for line in lines:
                        line = line[:-2]
                        entity = json.loads(line)
                        if 'entitytype' in entity and \
                            entity['entitytype'] == 'TEZ_DAG_ID' and \
                            'otherinfo' in entity:
                            otherinfo = entity['otherinfo']
                            if 'counters' in otherinfo:
                                counters = otherinfo['counters']
                                counterGroups = counters['counterGroups']
                                counters = counterGroups[0]['counters']
                                for item in counters:
                                    if item['counterName'] == 'TOTAL_LAUNCHED_TASKS':
                                        total_tasks = item['counterValue']
                        if 'entitytype' in entity and \
                            entity['entitytype'] == 'TEZ_TASK_ID' and \
                            'otherinfo' in entity and \
                            'events' in entity:
                            events = entity['events']
                            event = events[0]
                            if event['eventtype'] == 'TASK_FINISHED':
                                otherinfo = entity['otherinfo']
                                task_id = entity['entity']
                                task = OrderedDict()
                                task['startTime'] = otherinfo['startTime']
                                task['endTime'] = otherinfo['endTime']
                                task['status'] = otherinfo['status']
                                tasks_time[task_id] = task

        with open(query, 'w') as write_file:
            write_file.write('total_tasks: ' + str(total_tasks) + '\n')
            for id in tasks_time:
                write_file.write(id + ' ')
                item = tasks_time[id]
                for key in item:
                    write_file.write(key + ':' + str(item[key]) + ' ')
                write_file.write('\n')

        os.chdir(ori_path)
    os.chdir(cwd)

if __name__ == '__main__':
    processMR()
    processTez()