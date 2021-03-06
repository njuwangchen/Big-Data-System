import os
import shutil

data_file_root = '/afs/cs.wisc.edu/u/c/h/chenwang/838'
hadoop_history_root = data_file_root + '/hadoop-history'
tez_history_root = data_file_root + '/tez-history'
output_root = data_file_root + '/output'

def processMRFile(filename, queries, query_id_dict, query_completion_time):
    cwd = os.getcwd()
    filename = output_root + '/' + filename
    for q_num in queries:
        filename_new = filename+str(q_num)+'_mr.out'
        with open(filename_new) as mr_file:
            lines = mr_file.readlines()
            query_id_dict[q_num] = []
            time_taken_cnt = 0
            for line in lines:
                if line.startswith('Time taken'):
                    time_taken_cnt += 1
                    if time_taken_cnt == 2:
                        time = line.split(' ')[2]
                        query_completion_time[q_num] = time

                if line.startswith('Starting Job'):
                    job_id = line.split(' ')[3][:-1]
                    query_id_dict[q_num].append(job_id)
    os.chdir(cwd)

def writeResult(prefix, query_id_dict, query_completion_time, write_path):
    with open(write_path + '/' + prefix + '_query_job_id', 'w') as job_id_file:
        for query_id in query_id_dict:
            job_id_file.write(str(query_id) + ': ' + str(query_id_dict[query_id]) + '\n')

    with open(write_path + '/' + prefix + '_query_completion_time', 'w') as query_completion_file:
        for query_id in query_completion_time:
            query_completion_file.write(str(query_id) + ': ' + str(query_completion_time[query_id]) + '\n')

def processTezFile(filename, queries, query_id_dict, query_completion_time):
    cwd = os.getcwd()
    filename = output_root + '/' + filename
    for q_num in queries:
        filename_new = filename+str(q_num)+'_tez.out'
        with open(filename_new) as tez_file:
            lines = tez_file.readlines()
            query_id_dict[q_num] = []
            time_taken_cnt = 0
            for line in lines:
                if line.startswith('Time taken'):
                    time_taken_cnt += 1
                    if time_taken_cnt == 2:
                        time = line.split(' ')[2]
                        query_completion_time[q_num] = time

                if line.startswith('Status:'):
                    job_id = line.split(' ')[-1][:-2]
                    query_id_dict[q_num].append(job_id)
    os.chdir(cwd)

def collectHistoryFilesFor3(output3path):
    cwd = os.getcwd()
    path = output_root+'/'+output3path
    result_path = cwd + '/result/3'
    os.chdir(path)
    files = os.listdir(path)
    for file in files:
        if os.path.isfile(file):
            file_name = file[:file.index('.')]
            print file_name
            file_name_array = file_name.split('_')
            query = file_name_array[1]

            # what is the query with no query number?
            if len(query) < 7:
                continue

            query_num = int(query[-2:])
            query_type = file_name_array[2]

            query_id_dict = {}
            query_id_dict[query_num] = []
            with open(file) as data:
                lines = data.readlines()
                for line in lines:
                    if query_type == 'mr':
                        if line.startswith('Starting Job'):
                            job_id = line.split(' ')[3][:-1]
                            query_id_dict[query_num].append(job_id)
                    if query_type == 'tez':
                        if line.startswith('Status:'):
                            job_id = line.split(' ')[-1][:-2]
                            query_id_dict[query_num].append(job_id)
            cwd_tmp = os.getcwd()
            os.chdir(result_path)
            collectHistoryFiles(query_type, query_id_dict, True, file_name)
            os.chdir(cwd_tmp)
    os.chdir(cwd)

def collectHistoryFiles(prefix, query_id_dict, is3rd, name3rd):
    cwd = os.getcwd()
    print cwd
    if prefix == 'mr':
        cwd_his = cwd + '/mr_history'
        if not os.path.exists(cwd_his):
            os.mkdir(cwd_his)
        os.chdir(hadoop_history_root)
    elif prefix == 'tez':
        cwd_his = cwd + '/tez_history'
        if not os.path.exists(cwd_his):
            os.mkdir(cwd_his)
        os.chdir(tez_history_root)
    else:
        print "error collect history!"
        exit()
    print os.getcwd()
    for query_id in query_id_dict:
        print query_id
        if not is3rd:
            history_path = cwd_his + '/' +str(query_id)
            os.mkdir(history_path)
        else:
            history_path = cwd_his + '/' + name3rd
            os.mkdir(history_path)

        ids = query_id_dict[query_id]
        for dirpath, dirname, filenames in os.walk(os.getcwd()):
            for filename in filenames:
                if prefix == 'mr' and filename.split('-')[0] in ids and filename.endswith('.jhist'):
                    print filename
                    shutil.copy(dirpath + '/' + filename, history_path)

                if prefix == 'tez' \
                    and (filename.split('_')[1]+'_'+filename.split(('_'))[2]) == ids[0][12:]:

                    print filename
                    shutil.copy(dirpath + '/' + filename, history_path)
    os.chdir(cwd)

if __name__ == '__main__':
    filename = 'outputq1/tpcds_query'
    query_id_dict = {}
    query_completion_time = {}
    queries = [12, 21, 50, 71, 85]
    processMRFile(filename, queries, query_id_dict, query_completion_time)
    writeResult('mr', query_id_dict, query_completion_time, 'result/1')

    tez_query_id_dict = {}
    tez_query_completion_time = {}
    processTezFile(filename, queries, tez_query_id_dict, tez_query_completion_time)
    writeResult('tez', tez_query_id_dict, tez_query_completion_time, 'result/1')

    mr_history_path = 'mr_history'
    if os.path.exists(mr_history_path):
        shutil.rmtree(mr_history_path)
    tez_history_path = 'tez_history'
    if os.path.exists(tez_history_path):
        shutil.rmtree(tez_history_path)
    collectHistoryFiles('mr', query_id_dict, False, '')
    collectHistoryFiles('tez', tez_query_id_dict, False, '')

    if os.path.exists('result/3/'+mr_history_path):
        shutil.rmtree('result/3/'+mr_history_path)
    if os.path.exists('result/3/'+tez_history_path):
        shutil.rmtree('result/3/'+tez_history_path)
    collectHistoryFilesFor3('outputq3')


