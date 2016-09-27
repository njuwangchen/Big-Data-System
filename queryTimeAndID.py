def processMRFile(filename, queries, query_id_dict, query_completion_time):
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

def writeResult(prefix, query_id_dict, query_completion_time):
    with open(prefix + '_query_job_id', 'w') as job_id_file:
        for query_id in query_id_dict:
            job_id_file.write(str(query_id) + ': ' + str(query_id_dict[query_id]) + '\n')

    with open(prefix + '_query_completion_time', 'w') as query_completion_file:
        for query_id in query_completion_time:
            query_completion_file.write(str(query_id) + ': ' + str(query_completion_time[query_id]) + '\n')

def processTezFile(filename, queries, query_id_dict, query_completion_time):
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

if __name__ == '__main__':
    filename = 'queryTimeAndID/tpcds_query'
    query_id_dict = {}
    query_completion_time = {}
    queries = [12, 21, 50, 71, 85]
    processMRFile(filename, queries, query_id_dict, query_completion_time)
    writeResult('mr', query_id_dict, query_completion_time)

    tez_query_id_dict = {}
    tez_query_completion_time = {}
    processTezFile(filename, queries, tez_query_id_dict, tez_query_completion_time)
    writeResult('tez', tez_query_id_dict, tez_query_completion_time)
