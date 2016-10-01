import os
from collections import OrderedDict

data_file_root = '/afs/cs.wisc.edu/u/c/h/chenwang/838'
output_root = data_file_root + '/output'

def processParameterFiles(my_dir, mr_result, tez_result):
    cwd = os.getcwd()
    path = output_root + '/' + my_dir
    os.chdir(path)
    files = os.listdir(path)
    for file in files:
        if os.path.isfile(file):
            file_name = file[:file.rindex('.')]
            print file_name
            file_name_array = file_name.split('_')
            query = file_name_array[1]

            # what is the query with no query number?
            if len(query) < 7:
                continue

            query_num = int(query[-2:])
            query_type = file_name_array[2]

            item = OrderedDict()
            item['query'] = query_num
            for i in range(3, len(file_name_array)-1, 2):
                item[file_name_array[i]] = file_name_array[i+1]

            with open(file) as data:
                lines = data.readlines()
                time_taken_cnt = 0
                for line in lines:
                    if line.startswith('Time taken'):
                        time_taken_cnt += 1
                        if time_taken_cnt == 2:
                            time = line.split(' ')[2]
                            item['time'] = time

            print item
            if query_type == 'mr':
                mr_result.append(item)
            if query_type == 'tez':
                tez_result.append(item)

    os.chdir(cwd)

def writeParameterResult(my_dir, mr_result, tez_result):
    cwd = os.getcwd()
    path = my_dir
    os.chdir(path)
    mr_result = sorted(mr_result, key=lambda k: k['query'])
    tez_result = sorted(tez_result, key=lambda k: k['query'])
    with open('mr_result', 'w') as mr_file:
        for item in mr_result:
            for key in item:
                mr_file.write(key+':'+str(item[key])+' ')
            mr_file.write('\n')
    with open('tez_result', 'w') as tez_file:
        for item in tez_result:
            for key in item:
                tez_file.write(key + ':' + str(item[key])+' ')
            tez_file.write('\n')
    os.chdir(cwd)

if __name__ == '__main__':
    mr_result = []
    tez_result = []
    processParameterFiles('outputq2', mr_result, tez_result)
    writeParameterResult('result/2', mr_result, tez_result)

    mr_result_3 = []
    tez_result_3 = []
    processParameterFiles('outputq3', mr_result_3, tez_result_3)
    writeParameterResult('result/3', mr_result_3, tez_result_3)