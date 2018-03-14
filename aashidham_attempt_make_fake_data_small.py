import pandas as pd
import numpy as np

import os

os.system("mkdir aashidham_attempt_fake_data_small")

for i in range(10):
    userID = np.random.randint(100000, size=100)
    invoice_payment = np.random.randint(10, size=100)

    df = pd.DataFrame({'userID': userID, 'invoice_payment': invoice_payment})
    df.to_csv('aashidham_attempt_fake_data_small/{}.tmp'.format(i), index=False, header=False)
