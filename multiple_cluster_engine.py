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
                 input_file_names,
                 output_parent_dir,
                 function_to_process,
                 function_kwargs_dict,
                 cluster_job_name='MCE_job', 
                 n_cpus_list=(6, 4, 4), 
                 ram_limit_in_GB=20,
                 wait_time_in_seconds=1
                ): # always put function args in a dictionary
        reload(logging)
        self.input_file_names = input_file_names
        self.output_parent_dir = output_parent_dir
        self.function_to_process = lambda kwargs: function_to_process(**kwargs)
        self.function_kwargs_dict = function_kwargs_dict
        self.cluster_job_name = cluster_job_name
        self.n_cpus_list = n_cpus_list
        self.ram_limit_in_GB = ram_limit_in_GB
        self.wait_time_in_seconds = wait_time_in_seconds        
        
        assert len(self.input_file_names) > 0, "Need input files"
        assert os.path.isdir(self.output_parent_dir), "Output directory doesn't exist"
        assert cluster_job_name, "Needs cluster name"
        assert len(n_cpus_list) > 0, "Needs the number of CPUs per cluster"

        # used by engine
        self.cluster_dict = {}
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
        self.cluster_pid_dict = {}
        self.n_unsuccessful_files = 0

    def create_cluster_output_dir(self):
        """Creates the folder which all the results will be saved to based on
        the output_parent_dir and the cluster job name and an incremented number"""
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
        """Create logger objects"""
        l = logging.getLogger(logger_name)
        fileHandler = logging.FileHandler(log_file)
        l.addHandler(fileHandler)
        l.setLevel(logging.INFO)
    
    def activate_logger(self):
        """Create a logger for status, failure, and RAM usage updates"""
        self.create_logger('status', os.path.join(self.cluster_output_dir, "status.log"))
        self.create_logger('failure', os.path.join(self.cluster_output_dir, "failure.log"))
        self.create_logger('ram_usage', os.path.join(self.cluster_output_dir, "ram_usage.log"))
        self.logger_status = logging.getLogger('status')
        self.logger_status.propagate = False
        self.logger_failure = logging.getLogger('failure')
        self.logger_failure.propagate = False
        self.logger_ram_usage = logging.getLogger('ram_usage')
        self.logger_ram_usage.propagate = False
        
    def profile_memory_for_cluster(self, cluster_id): # RAM in GB
        """Given a cluster ID, determines how much RAM that cluster is using,
        which is just the sum of the RAM attached the CPUs in the cluster"""
        return sum(psutil.Process(pid).memory_info().rss for 
                   pid in self.cluster_pid_dict[cluster_id]) / 1e9
        
    def profile_memory_for_all_clusters(self):
        """Determines the RAM for all the clusters"""
        self.cluster_RAM_use_dict.clear()
        for jth_cluster in sorted(self.load_balanced_view_dict):
            self.cluster_RAM_use_dict[jth_cluster] = self.profile_memory_for_cluster(jth_cluster)           
            self.logger_ram_usage.info('{}: {}th cluster uses {} GB of RAM'.format(
                datetime.now(), jth_cluster, self.cluster_RAM_use_dict[jth_cluster]))
        self.logger_ram_usage.info('{}: All clusters use {} GB of RAM'.format(
                datetime.now(), sum(self.cluster_RAM_use_dict.values())))
        
    def clear_memory_on_cluster(self, cluster_id): # not as effective as imagined
        """Ideally clears out the RAM when a cluster successfully processes a file.
        In practice, it is hard to say how effective this is in clearing the RAM.
        Fortunately, running this method is very fast"""
        import gc
        self.cluster_dict[cluster_id][:].apply_async(gc.collect)
        
    def start_cluster(self, n_cpus, cluster_id):
        """Given a cluster ID, start the cluster with that ID and then attach
        to the cluster and then store the CPU PIDs cluster_pid_dict and then
        log it in the status.log file
        """
        self.logger_status.info("{}: \tAttempting to start cluster job "
            "{}'s {}th cluster with {} CPUs".format(datetime.now(), 
            self.cluster_job_name, cluster_id, n_cpus))
        os.system("ipcluster start --n={} --profile={}{} --daemonize".format(
            n_cpus, self.cluster_job_name, cluster_id)) # should deprecate to use a safer OS call

        attempt_ctr = 0 
        while attempt_ctr < 3: # Attempt to connect to client 3 times
            time.sleep(10) # hard coded
            try:
                cluster = ipp.Client(profile='{}{}'.format(self.cluster_job_name, cluster_id))
            except ipp.error.TimeoutError:
                attempt_ctr += 1
            else:
                self.cluster_pid_dict[cluster_id] = cluster[:].apply_async(os.getpid).get()
                self.logger_status.info(('{}: \t\tCPU processes ready for action'
                    ': {}').format(datetime.now(), self.cluster_pid_dict[cluster_id]))
                return cluster
            # if there is any other error other than TimeoutError, then the error will be raised
            
    def start_all_clusters(self):
        """Starts all the clusters specified and also writes updates to status.log"""
        self.activate_logger()
        self.logger_status.info('{}: Starting Multiple Cluster Engine'.format(datetime.now()))
        for cluster_id, n_cpus in enumerate(self.n_cpus_list):
            self.cluster_dict[cluster_id] = self.start_cluster(n_cpus, cluster_id)
            self.load_balanced_view_dict[cluster_id] = self.cluster_dict[cluster_id].load_balanced_view()            
        self.start_time = datetime.now()
        self.logger_status.info('{}: All clusters started at {}'.format(datetime.now(), self.start_time))
        self.cluster_indexes = itertools.cycle(sorted(self.load_balanced_view_dict))
        
    def kill_cluster(self, cluster_id):
        """Given a cluster ID, kills that cluster and write update to status.log and
        update cluster_indexes to know which clusters are remaining"""
        self.logger_status.info(("{}: \tAttempting to kill cluster job {}'s {}th "
            "cluster with CPU processes: {}").format(datetime.now(), 
            self.cluster_job_name, cluster_id, self.cluster_pid_dict[cluster_id]))
        self.load_balanced_view_dict.pop(cluster_id)
        # cluster.purge_everything() # sometimes this line takes forever
        self.cluster_dict[cluster_id].close()
        self.cluster_dict.pop(cluster_id)
        os.system('ipcluster stop --profile={}{}'.format(self.cluster_job_name, cluster_id))
        self.logger_status.info('{}: \t\tCluster successfully killed'.format(datetime.now()))
        self.cluster_indexes = itertools.cycle(sorted(self.load_balanced_view_dict))
        time.sleep(5) # hard-coded
        
    def kill_all_clusters(self):
        """Kills all clusters that are remaining and writes updates to status.log"""
        self.end_time = datetime.now()
        n_surviving_clusters = len(self.cluster_dict)
        self.logger_status.info('{}: Killing all remaining clusters'.format(datetime.now()))
        for cluster_id in sorted(self.cluster_dict):
            self.kill_cluster(cluster_id)
        self.logger_status.info('{}: All clusters have been killed'.format(datetime.now()))
        self.logger_status.info('{}: Multiple Cluster Engine shut down at {}'.format(
            datetime.now(), self.end_time))
        self.logger_status.info(("{}: Appears that {} files were successfully "
            "processed using {} surviving clusters in {} minutes").format(
            datetime.now(), len(self.input_file_names) - self.n_unsuccessful_files, 
            n_surviving_clusters, (self.end_time - self.start_time).seconds / 60.0))
        logging.shutdown()
        
    def early_kill_cluster(self, cluster_id):
        """Given a cluster ID, kills that cluster and writes which file that cluster
        was working on to failure.log and updates status and RAM logs. An early cluster
        kill happens when total RAM usage exceeds the limit set by the user"""
        self.logger_failure.info(("{}: Killing cluster job {}'s {}th cluster which "
            "was processing file {} due to exceeding RAM limit").format(
            datetime.now(), self.cluster_job_name, cluster_id, 
            self.file_to_cluster_order_dict[cluster_id][-1]))
        self.logger_ram_usage.info(("{}: Killing cluster job {}'s {}th cluster due "
            "to exceeding RAM limit").format(datetime.now(), self.cluster_job_name, cluster_id))        
        self.logger_status.info(("{}: Killing cluster job {}'s {}th cluster with CPU "
            "processes: {} due to exceeding RAM limit").format(datetime.now(), 
            self.cluster_job_name, cluster_id, self.cluster_pid_dict[cluster_id]))
        self.cluster_dict[cluster_id].close()
        os.system('ipcluster stop --profile={}{}'.format(self.cluster_job_name, cluster_id))
        self.load_balanced_view_dict.pop(cluster_id)
        self.cluster_dict.pop(cluster_id)
        self.async_results_dict.pop(cluster_id)
        self.n_unsuccessful_files += 1
        
    def kill_cluster_if_ram_limit_exceeded(self): 
        """Checks if total RAM used exceeds limit set by user. If so, kill the 
        cluster that uses the most RAM and update some logs. Only kills at 
        max 1 cluster per method call"""
        if sum(self.cluster_RAM_use_dict.values()) > self.ram_limit_in_GB:
            cluster_id = sorted(self.cluster_RAM_use_dict, 
                                 key=self.cluster_RAM_use_dict.get, reverse=True)[0]
            self.early_kill_cluster(cluster_id)
            self.cluster_indexes = itertools.cycle(sorted(self.load_balanced_view_dict))
        if len(self.load_balanced_view_dict) == 0:
            self.logger_failure.info(("{}: All clusters have been killed prematurely "
                "(probably due to exceeding RAM limit), so it would be a good idea"
                " to determine which files, if any, successfully processed"
                 ).format(datetime.now()))
            self.logger_status.info(("{}: All clusters have been killed prematurely "
                "(probably due to exceeding RAM limit)").format(datetime.now()))
            raise Exception('All clusters have been killed prematurely')
    
    def check_if_function_in_cluster_failed(self, cluster_id):
        """Given a cluster ID, checks if the most recent file (sent to
        that cluster) successfully processed or not. If there was an error
        in processing, then write the error to failure.log and remove
        its async_result history"""
        if self.async_results_dict[cluster_id] == []: # cluster just started, so it
            return # doesn't have any files sent to the cluster yet
        else:
            exception = self.async_results_dict[cluster_id][-1].exception()
            if exception:
                self.logger_failure.info(("{}: {}th cluster has error {} on "
                    "file {}").format(datetime.now(), cluster_id, exception.args[0], 
                    self.file_to_cluster_order_dict[cluster_id][-1]))
                self.async_results_dict[cluster_id].pop()
                self.n_unsuccessful_files += 1

    def create_kwargs_dict_list(self, input_file_name, cluster_id, n_cpus):
        """Packages up the arguments into a dictionary to be sent to each cluster. 
        There are function arguments as well as cluster arguments (cluster ID and 
        number of CPUs in that cluster). If the cluster has n CPUs, then create
        a list of n copies of this kwargs dictionary"""
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
                
    def run_clusters(self):
        """Iterates through all the files. You want to order your files from largest
        to smallest. The reason is that the largest files (file0, file1, file2, etc)
        will be sent to the largest cluster/cluster with the most CPUs (which 
        we will your first cluster). For each file, create a kwargs dictionary to 
        be sent to the cluster, write to status.log which file is going to which
        cluster, and store the async_result in async_results_dict. You can also
        inspect the order of files sent to which clusters by checking 
        file_to_cluster_order_dict after the engine has finished processing
        all the files. For every wait_time_in_seconds, check if a cluster is
        finished processing its current file and available to send the next
        file. In addition, for every wait_time_in_seconds, check if RAM usage
        exceeds limit set by user. If so, kill the cluster using the most RAM"""
        small_file_ctr = 1 # effectively a dequeue scheme
        big_file_ctr = 0
        for ith_file in tqdm(range(len(self.input_file_names))):
            while True:
                time.sleep(self.wait_time_in_seconds)
                self.profile_memory_for_all_clusters()
                self.kill_cluster_if_ram_limit_exceeded()
                jth_cluster = next(self.cluster_indexes)                
                if (not self.async_results_dict[jth_cluster][-1:]
                        or self.async_results_dict[jth_cluster][-1].done()): # check if cluster j is available                       
                    self.clear_memory_on_cluster(jth_cluster)
                    self.check_if_function_in_cluster_failed(jth_cluster) # check if previous file failed to process
                    if jth_cluster == 0: # send large files to large cluster (which ALWAYS has id == 0)
                        index = big_file_ctr
                        big_file_ctr += 1
                    else: # send small files to small clusters (which ALWAYS have id > 0)
                        index = -small_file_ctr
                        small_file_ctr += 1
                                                                                   
                    kwargs_dict_list = self.create_kwargs_dict_list(
                        self.input_file_names[index],
                        jth_cluster, 
                        len(self.cluster_dict[jth_cluster].ids))                    
                    
                    async_result = self.load_balanced_view_dict[jth_cluster].map_async(
                        self.function_to_process, kwargs_dict_list)                                              
                    self.async_results_dict[jth_cluster].append(async_result)
                    self.file_to_cluster_order_dict[jth_cluster].append(self.input_file_names[index])
                    # write status to file--it will only have start times, no end times
                    self.logger_status.info(("{}: {} is the {}th file and is sent to "
                        "{}th cluster for processing").format(datetime.now(),
                        self.input_file_names[index], ith_file, jth_cluster))
                    break # break out of inner loop to determine if other clusters are available

        while not all(self.async_results_dict[jth_cluster][-1].done()
                      for jth_cluster in self.async_results_dict): # wait for all clusters to finish
            time.sleep(self.wait_time_in_seconds)
            self.profile_memory_for_all_clusters()
            self.kill_cluster_if_ram_limit_exceeded()
                
        cluster_set = set()
        for jth_cluster in self.cluster_indexes:
            if jth_cluster in cluster_set:
                break
            cluster_set.add(jth_cluster)
            self.check_if_function_in_cluster_failed(jth_cluster) # check if last file failed to process
        # async_results_dict; save to disk for later inspection? determine whether results takes too much RAM
        
    def main(self):
        """Runs the entire thing"""
        self.create_cluster_output_dir()
        self.start_all_clusters()
        self.run_clusters()
        self.kill_all_clusters()