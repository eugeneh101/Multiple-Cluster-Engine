def user_invoice_total(
    # mandatory args, you can choose not to use them but function has to take them in
     input_file_name, cluster_output_dir, cluster_id, n_cpus, cpu_id  
):
    import os # function has to import all the libraries it uses
    from collections import defaultdict
    import gc
    import pandas as pd
    
    def mapper(input_file_name, n_cpus, cpu_id):
        data_dict = defaultdict(list)
        with open(input_file_name, 'r') as in_:
            for line in in_:
                line = line.strip().split(',')
                userID, invoice_payment = int(line[0]), int(line[1])
                if hash(userID) % n_cpus == cpu_id:
                    data_dict[userID].append(invoice_payment)
        return data_dict
    
    def reducer(data_dict, cluster_output_dir, cluster_id, cpu_id):
        results_dict = {}
        for userID in sorted(data_dict):
            data = data_dict.pop(userID) # reduces data_dict size
            results_dict[userID] = [pd.DataFrame(data).sum()[0]] # only create a DataFrame for 1 userID at a time
            
        output_file_name = '_'.join(['cluster' + str(cluster_id), 
             'cpu' + str(cpu_id), input_file_name.split('/')[-1]])
        output_file_name = os.path.join(cluster_output_dir, output_file_name)
        pd.DataFrame(results_dict).T.to_csv(output_file_name, header=False)

    
    data_dict = mapper(input_file_name, n_cpus, cpu_id)
    gc.collect() # garbage collector might reduce RAM usage; quick to run
    reducer(data_dict, cluster_output_dir, cluster_id, cpu_id)


from multiple_cluster_engine import MultipleClusterEngine
from glob import glob


input_file_names = glob('aashidham_attempt_fake_data_small/*')
print input_file_names
mce_args = {
    'cluster_job_name': 'get_user_invoice_total_', # no spaces as it will be part of directory name
    'n_cpus_list': [4, 3, 2], # 1st cluster is always the largest or equal to the other clusters
    'ram_limit_in_GB': 20.0,
    'wait_time_in_seconds': 1,
    'input_file_names': input_file_names,
    'output_parent_dir': '/home/ubuntu/cluster_results', # use absolute path since it's safer, has to already exist
    'function_to_process': user_invoice_total,
    'function_kwargs_dict': {} # user_invoice_total() takes no additional arguments    
    }

mce5 = MultipleClusterEngine(**mce_args)
mce5.main() # I'm expecting an Exception here due to killing all clusters




