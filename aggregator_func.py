import pandas as pd

def get_data_dict(file_name, num_cpus, cpu_id):
    from collections import defaultdict
    
    month_data = defaultdict(list)
    with open(file_name) as f:
        column_names = f.readline().strip().split(', ')
        id_column = column_names.index('invoice_id')
        for line in f:
            line = line.strip().split(', ')
            if hash(line[id_column]) % num_cpus != cpu_id:
                continue
            month_data[line[id_column]].append(line[1:])
        return month_data, column_names[1:]
                
def get_monthly_summary(unique_id, user_data, column_names):
    import pandas as pd
    
    df = pd.DataFrame(user_data, columns=column_names) # column names hard-coded
    mean_sum = df['sum'].astype('float').mean()
    mean_weight = df['weight'].astype('float').mean()
    mean_time = df['time'].astype('float').mean()
    return (unique_id, mean_sum, mean_weight, mean_time)
    

def process_file(file_name, num_cpus, cpu_id):
    month_data, column_names = get_data_dict(file_name, num_cpus, cpu_id)
    final_results = []
    for unique_id in month_data.keys(): # this would not work in Python 3 due to view object
        final_results.append(get_monthly_summary(unique_id, month_data[unique_id], column_names))


    df_results = pd.DataFrame(final_results)
    output_file = 'results_for_' + file_name.strip('.txt') + '_cpu_{}.txt'.format(cpu_id)
    df_results.round(decimals=3).to_csv(output_file, index=False)


#month_data, column_names = get_data_dict('invoices0.txt', 2, 1) # hard-coded arguments
#process_file('invoices0.txt', 2, 1)