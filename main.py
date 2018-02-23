from collections import defaultdict
from datetime import datetime 
import itertools, argparse
import time
from glob import glob
import ipyparallel as ipp
import os

from aggregator_func import process_file


parser = argparse.ArgumentParser(description='Multi-cluster platform for aggregation of data files.')
parser.add_argument('integers', metavar='N', type=int, nargs='*', help='number of cores for this cluster')
arr0 = parser.parse_args().integers

# Start a cluster with specified number of cpus
def start_cluster(cluster_id, num_cpus):
    import time
    #Test if cluster is started correctly. Use system module warnings.
    # !ipcluster start --n={num_cpus} --profile={'mycluster' + str(cluster_id)} --daemonize
    import os
    os.system("ipcluster start --n={} --profile={} --daemonize".format(
        num_cpus, 'mycluster' + str(cluster_id)))
#    print("started profile={}".format('mycluster' + str(cluster_id)))
    
    #Attempt to connect to client 10 times
    attempt_ctr = 0 
    while attempt_ctr < 10:
        time.sleep(20)
        try:
            client = ipp.Client(profile='mycluster' + str(cluster_id))
            break
        except:
            attempt_ctr += 1
            pass
    print('\nstarted CPU processes: {}:'.format(client[:].apply_async(os.getpid).get()))
    return client

# Detach client from cluster
def kill_cluster(cluster_id, client_list):
    import time
    import os
    client = client_list[cluster_id]
    print('attempting to kill CPU processes: {}:'.format(client[:].apply_async(os.getpid).get()))
    #client.purge_everything()
    client.close()
    cluster = "mycluster{}".format(cluster_id)
    # !ipcluster stop --profile={cluster}
    os.system('ipcluster stop --profile=mycluster{}'.format(str(cluster_id)))
    time.sleep(20)




# Choose appropriate files folder, based on emitter type
file_names = ['invoices0.txt', 'invoices1.txt']
num_files = len(file_names)
print 'There are ' + str(num_files) + ' files to process'
assert num_files > 0, 'File list empty'

# Define global computing parameters
# There is one big cluster for 50gb+ files (if present) and two clusters for smaller files.
cpu_list = [1,1]
if len(arr0):
    cpu_list = arr0
num_clusters = len(cpu_list)
client_list = []
load_balanced_view_list = []

# Start your engines!
for i in range(num_clusters):
    client_list.append(start_cluster(i, cpu_list[i]))
    load_balanced_view_list.append(client_list[i].load_balanced_view())

# Create asynchronous object and cycle
my_async_results_list = defaultdict(list)
cluster_index = itertools.cycle(range(len(load_balanced_view_list)))

# Progress check!
for i in range(num_clusters):
    print 'Client ids for cluster ' + str(i) + ': ' + str(client_list[i].ids)
print str(num_clusters) + ' clusters activated!'
print '\n Client connection checkpoint reached!'

# Record total processing time
now = datetime.today()

# Distribute small and large files to small and large clusters
small_file_ctr = 1
big_file_ctr = 0
for file_idx in range(len(file_names)):
    for i in cluster_index: # infinite loop
        time.sleep(1) # hard coded delay time
                    
        if (not my_async_results_list[i][-1:] or my_async_results_list[i][-1].done()): # check if cluster i is available           
            num_cpus = len(client_list[i].ids)
        
            # Must de-bug cluster shutdown code
            kill_cluster(i, client_list)
                
            #For de-bugging
            print 'stopped cluster '+ str(i)
            client_list[i] = start_cluster(i, num_cpus)
            load_balanced_view_list[i] = client_list[i].load_balanced_view()
            
            # Send large files to large cluster (ALWAYS has id == 0)
            if i == 0: 
                index = big_file_ctr
                big_file_ctr += 1
            # Send small files to small clusters (ALWAYS have id > 0)
            else:    
                index = -small_file_ctr
                small_file_ctr += 1
                
            print "file is {}, {}".format(file_names[index], index)
            temp = load_balanced_view_list[i].map_async(
                    process_file, # function name
                    [file_names[index]] * len(client_list[i].ids), # file name
                    [len(client_list[i].ids)] * len(client_list[i].ids), # number of CPUs
                    client_list[i].ids # CPU ids
                    ) 
            my_async_results_list[i].append(temp)
            print "File is sent to cluster {}".format(i) 
            break
        
# kill all existing clusters
for i in range(len(client_list)):
    kill_cluster(i, client_list)        

small_file_ctr -= 1
with open('time_stamp.txt', 'a') as f:
    f.write(str(datetime.today()) + '\n')
    to_write = 'Processed ' + str(num_files) + ' files in ' \
            + str((datetime.today() - now).total_seconds() / 60) + ' minutes\n'
    to_write += 'Used ' + str(cpu_list[0]) + ' cpus to process ' + str(big_file_ctr) + \
                    ' big files and ' 
    # If there are multiple clusters, specify number of small files processed by small cluster
    if len(cpu_list) > 1: 
        to_write += str(cpu_list[1]) + ' cpus to process ' + \
                    str(small_file_ctr) + ' small files.\n'
    to_write += 'A total of ' + str(len(cpu_list)) + ' clusters were used.\n'
    if len(cpu_list) > 1: 
        to_write += 'Expect ' + str(big_file_ctr * cpu_list[0] + small_file_ctr * cpu_list[1]) + \
                    ' files in processed folder.\n\n'
    else:
        to_write += 'Expect ' + str(big_file_ctr * cpu_list[0]) + \
                    ' files in processed folder.\n\n'
    f.write(to_write)
    
print 'Done processing!'