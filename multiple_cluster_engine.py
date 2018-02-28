import ipyparallel as ipp
from collections import defaultdict
import os
import psutil
import time
from datetime import datetime
from tqdm import tqdm
import copy
import itertools

import logging # logging can create duplicate entries if you don't reload logging
try:
    from importlib import reload # Python 3
except: # Python 2 reload is a builtin
    pass

class MultipleClusterEngine(object):
    def __init__(self, 
                 cluster_job_name, 
                 n_cpus_list, 
                 input_file_names,
                 output_parent_dir,
                 function_to_process,
                 function_kwargs_dict): # always put function args in a dictionary
        reload(logging)
        self.cluster_job_name = cluster_job_name
        self.n_cpus_list = n_cpus_list
        self.output_parent_dir = output_parent_dir
        self.input_file_names = input_file_names
        self.function_to_process = lambda kwargs: function_to_process(**kwargs)
        self.function_kwargs_dict = function_kwargs_dict
        
        assert cluster_job_name, "Needs cluster name"
        assert len(n_cpus_list) > 0, "Needs the number of CPUs per cluster"
        assert os.path.isdir(self.output_parent_dir), "Output directory doesn't exist"
        assert len(self.input_file_names) > 0, "Need input files"

        # used by engine
        self.client_dict = {}
        self.load_balanced_view_dict = {}
        self.async_results_dict = defaultdict(list) # collects all the async_results
        self.file_to_cluster_order_dict = defaultdict(list) # remembers which file is sent to which cluster
        self.cluster_indexes = None
        self.logger_status = None
        self.logger_failure = None
        self.start_time = None
        self.end_time = None
        self.cluster_output_dir = None
        self.cluster_RAM_use_dict = {}

    def create_cluster_output_dir(self):
        subdirs = [name for name in os.listdir(self.output_parent_dir) if 
                   os.path.isdir(os.path.join(self.output_parent_dir, name))]
        existing_results_dir = []
        for subdir in subdirs:
            try:
                existing_results_dir.append(int(subdir.strip(self.cluster_job_name)))
            except ValueError:
                pass
        dir_index = max(existing_results_dir) + 1 if existing_results_dir else 0
        self.cluster_output_dir = os.path.join(self.output_parent_dir, 
                                               self.cluster_job_name + str(dir_index))
        os.makedirs(self.cluster_output_dir)
                
    def create_logger(self, logger_name, log_file):
        l = logging.getLogger(logger_name)
        fileHandler = logging.FileHandler(log_file)
        l.addHandler(fileHandler)
        l.setLevel(logging.INFO)
    
    def activate_logger(self):
        self.create_logger('status', os.path.join(self.cluster_output_dir, "status.log"))
        self.create_logger('failure', os.path.join(self.cluster_output_dir, "failure.log"))
        self.create_logger('ram_usage', os.path.join(self.cluster_output_dir, "ram_usage.log"))
        self.logger_status = logging.getLogger('status')
        self.logger_status.propagate = False
        self.logger_failure = logging.getLogger('failure')
        self.logger_failure.propagate = False
        self.logger_ram_usage = logging.getLogger('ram_usage')
        self.logger_ram_usage.propagate = False
        
    def profile_memory_for_cluster(self, jth_cluster): # RAM in GB
        cluster_pids = self.client_dict[jth_cluster][:].apply_async(os.getpid).get()
        return sum(psutil.Process(pid).memory_info().rss for pid in cluster_pids) / 1e9
        
    def profile_memory_for_all_clusters(self):
        self.cluster_RAM_use_dict.clear()
        for jth_cluster in sorted(self.load_balanced_view_dict):
            self.cluster_RAM_use_dict[jth_cluster] = self.profile_memory_for_cluster(jth_cluster)           
            self.logger_ram_usage.info('{}: {}th cluster uses {} GB of RAM'.format(
                datetime.now(), jth_cluster, self.cluster_RAM_use_dict[jth_cluster]))
        self.logger_ram_usage.info('{}: All clusters uses {} GB of RAM'.format(
                datetime.now(), sum(self.cluster_RAM_use_dict.values())))
        # if all clusters are dead, then raise Error with a message
            
    def early_kill_cluster(self, jth_cluster):
        pass
        
    
    def start_cluster(self, n_cpus, cluster_id):
        self.logger_status.info("\tAttempting to start cluster job "
                "{}'s {}th cluster with {} CPUs".format(self.cluster_job_name, cluster_id, n_cpus))
        os.system("ipcluster start --n={} --profile={}{} --daemonize".format(
            n_cpus, self.cluster_job_name, cluster_id)) # should deprecate to use a safer bash call

        attempt_ctr = 0 
        while attempt_ctr < 3: # Attempt to connect to client 3 times
            time.sleep(10) # hard coded
            try:
                client = ipp.Client(profile='{}{}'.format(self.cluster_job_name, cluster_id))
            except ipp.error.TimeoutError:
                attempt_ctr += 1
            else:
                self.logger_status.info('\t\tCPU processes ready for action: {}'.format(
                    client[:].apply_async(os.getpid).get()))
                return client
            # if there is any other error other than TimeoutError, then the error will be raised
            
    def start_all_clusters(self):
        self.activate_logger()
        self.logger_status.info('Starting Multiple Cluster Engine')
        #self.logger_status.info('Attempting to start all clusters')
        for cluster_id, n_cpus in enumerate(self.n_cpus_list):
            self.client_dict[cluster_id] = self.start_cluster(n_cpus, cluster_id)
            self.load_balanced_view_dict[cluster_id] = self.client_dict[cluster_id].load_balanced_view()            
        self.start_time = datetime.now()
        self.logger_status.info('All clusters started at {}'.format(self.start_time))
        self.cluster_indexes = itertools.cycle(sorted(self.load_balanced_view_dict))
        
    def kill_cluster(self, cluster_id): # use better arguments
        self.logger_status.info('\tAttempting to kill {}{} with CPU processes: {}'.format(
            self.cluster_job_name, cluster_id, self.client_dict[cluster_id][:].apply_async(os.getpid).get()))
        self.load_balanced_view_dict.pop(cluster_id)
        # client.purge_everything()
        self.client_dict[cluster_id].close()
        os.system('ipcluster stop --profile={}{}'.format(self.cluster_job_name, cluster_id))
        self.logger_status.info('\t\tCluster successfully killed')
        time.sleep(5) # hard-coded
        # have to mutate cluster_indexes
        
    def kill_all_clusters(self):
        self.end_time = datetime.now()
        self.logger_status.info('Killing all clusters')
        for cluster_id in self.client_dict:
            self.kill_cluster(cluster_id)
        self.logger_status.info('All clusters have been killed')
        self.logger_status.info('Multiple Cluster Engine shut down at {}'.format(self.end_time))
        self.logger_status.info('Processed {} files in {} minutes'.format(
            len(self.input_file_names), (self.end_time - self.start_time).seconds / 60.0))
        logging.shutdown()

    def create_kwargs_dict_list(self, input_file_name, cluster_id, n_cpus):
        function_kwargs_dict = copy.deepcopy(self.function_kwargs_dict)
        function_kwargs_dict.update({'input_file_name': input_file_name,
                                    'cluster_output_dir': self.cluster_output_dir,
                                    'cluster_id': cluster_id,
                                    'n_cpus': n_cpus})
        function_kwargs_dict_list = []
        for cpu_id in range(n_cpus):
            function_kwargs_dict_list.append(copy.deepcopy(function_kwargs_dict))
            function_kwargs_dict_list[cpu_id]['cpu_id'] = cpu_id
        return function_kwargs_dict_list 
    
    def check_if_function_in_cluster_failured(self, jth_cluster):
        if self.async_results_dict[jth_cluster] == []: # cluster just started, so it
            return # doesn't have any files sent to the cluster yet
        else:
            exception = self.async_results_dict[jth_cluster][-1].exception()
            if exception:
                self.logger_failure.info('{}th cluster has error {} on file {}'.format(
                    jth_cluster, exception.args[0], self.file_to_cluster_order_dict[jth_cluster][-1]))
                                     
    def run_clusters(self):
        small_file_ctr = 1 # determine if you want to have queue or differently ordered queue
        big_file_ctr = 0
        
        for ith_file in tqdm(range(len(self.input_file_names))):
            for jth_cluster in self.cluster_indexes: # infinite loop
                time.sleep(1) # hard coded delay time; want to do expected log time lag
                ### insert code here to kill cluster if RAM usage too great, 
                ###       if possible log which file it was processing;
                self.profile_memory_for_all_clusters()                
                
                if (not self.async_results_dict[jth_cluster][-1:] 
                    or self.async_results_dict[jth_cluster][-1].done()): # check if cluster i is available                       
                    # if necessary, recreate engine here if cluster shut down
                    # clear cluster memory
                    self.check_if_function_in_cluster_failured(jth_cluster) # check if previous file failed to process
                    
                    if jth_cluster == 0: # Send large files to large cluster (ALWAYS has id == 0)
                        index = big_file_ctr
                        big_file_ctr += 1
                    else: # Send small files to small clusters (ALWAYS have id > 0)
                        index = -small_file_ctr
                        small_file_ctr += 1
                                                                                   
                    kwargs_dict_list = self.create_kwargs_dict_list(
                        self.input_file_names[index],
                        jth_cluster, 
                        len(self.client_dict[jth_cluster].ids))                    
                    
                    async_result = self.load_balanced_view_dict[jth_cluster].map_async(
                        self.function_to_process, kwargs_dict_list)                                              
                    self.async_results_dict[jth_cluster].append(async_result)
                    self.file_to_cluster_order_dict[jth_cluster].append(self.input_file_names[index])
                    # write status to file--it will only have start times, no end times
                    self.logger_status.info(("{} is the {}th file and is sent to "
                        "{}th cluster for processing").format(
                        self.input_file_names[index], ith_file, jth_cluster))
                    break # break out of inner loop to determine if other clusters are available

        while not all(self.async_results_dict[jth_cluster][-1].done()
                      for jth_cluster in self.async_results_dict): # wait for all clusters to finish
            time.sleep(1)
            
        cluster_set = set()
        for jth_cluster in self.cluster_indexes:
            if jth_cluster in cluster_set:
                break
            cluster_set.add(jth_cluster)
            self.check_if_function_in_cluster_failured(jth_cluster)
        # async_results_dict; save to disk for later inspection?
        
    def main(self):
        self.create_cluster_output_dir()
        self.start_all_clusters()
        self.run_clusters()
        self.kill_all_clusters()