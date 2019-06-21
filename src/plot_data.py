import pandas as pd
from scipy.signal import correlate
import numpy as np
from numpy import ma
import os
import matplotlib.pyplot as plt
from pyproj import Proj
from pykalman import KalmanFilter
import seaborn as sns


# set up paths
basepath = '/OSM/HBA/OA_BENTHICVIDEO/archive/surveys/IN2018_V06/MRITC'
inputpath = basepath +'/data/input/'

operations = pd.read_csv(inputpath+'operations.csv',
                         index_col='Operation', parse_dates=['StartTime', 'EndTime', 'Duration'])



def task_plot_data_volume():
    def plot_data_volume(row,dependencies,targets):
        summary =[]
        files = list(dependencies)
        files.sort()
        for file in files:
            fszie=0
            lines=0
            rate =0
            datarate =0
            if os.path.exists(file):
                fsize=os.stat(file).st_size
                data = pd.read_csv(file,index_col=['timestamp'],parse_dates=['timestamp'])
                if len(data)>2:
                    rate = (data.index[-1]-data.index[0]).total_seconds()
                    lines =len(pd.read_csv(file))
                    datarate = lines /rate
            summary.append({
                'type':os.path.basename(file).split('_')[1],
                'size':fsize,
                'rate': datarate,
                'lines':lines,
                        })
        data = pd.DataFrame(summary)
        data.set_index('type',inplace=True)
        fig, ax = plt.subplots(figsize=(8, 8))
        data['rate'].plot.bar(ax=ax,color=plt.cm.Paired(np.arange(7)))
        ax.set_title('Operation %d' %(row.name))
        ax.set_ylim([0,2])
        ax.grid()
        plt.savefig(list(targets)[0])
    for ind, row in operations.iterrows():
        file_dep = []
        for groups in ['IMU', 'CTD', 'USB', 'ALT', 'LAB','SAS','IMG']:
            file_dep.append(basepath + '/IN2018_V06_%03d/data/MRITC_%s_IN2018_V06_%03d.CSV' % (ind,groups, ind))
        file_dep = list(filter(lambda file: os.path.exists(file), file_dep))
        yield {
            'name': str(ind),
            'actions': [(plot_data_volume, [],{'row':row} )],
            # 'task' keyword arg is added automatically
            'targets': [basepath+'/plots/rawdata/MRITC_LAB_IN2018_V06_%03d.jpg' %(ind)],
            'file_dep':file_dep,
            'uptodate': [False, ],
            'clean': True,
        }

if __name__ == '__main__':
    import doit

    #print(globals())
    doit.run(globals())