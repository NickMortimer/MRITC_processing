
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
inputpath = basepath +'/input/'

operations = pd.read_csv(inputpath+'operations.csv',
                         index_col='Operation', parse_dates=['StartTime', 'EndTime', 'Duration'])


def make_path(item):
    return basepath+'/IN2018_V06_%03d/data/MRITC_LBV_IN2018_V06_%03d.csv' % (item.name, item.name)
operations['path'] = operations.apply(make_path, axis=1)



# %%
images = pd.read_json('/OSM/HBA/OA_BENTHICVIDEO/archive/images/IN2018_V06/sensor_data/image_xif2no.json', lines=True)
images.head()
images.index = pd.to_datetime(images.SubSecCreateDate, format='%Y:%m:%d %H:%M:%S.%f')
images.SubSecCreateDate = images.index
images.sort_index(inplace=True)
images['Name'] = images.SourceFile.apply(os.path.basename)
parts = images['Name'].str.split('_', expand=True)
images['Camera'] = parts[1]
images['Operation'] = parts[4]
images['Operation'] = images['Operation'].astype(int)
images['Number'] = parts[6].str.split('.', expand=True)[0]
images.index.name = 'timestamp'



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
        data['rate'].plot.bar(ax=ax)
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
            'uptodate': [True, ],
            'clean': True,
        }






# %%
def task_write_image_data():
    def write_image(item,targets):
        images.loc[(images.index > item.StartTime) & (images.index < item.EndTime),'Operation'] =item.name
        images[(images.index > item.StartTime) & (images.index < item.EndTime)].to_csv(list(targets)[0])

    for ind, row in operations.iterrows():
        yield {
            'name': str(ind),
            'actions': [(write_image, [], {'item': row})],
            # 'task' keyword arg is added automatically
            'targets': [
                basepath+'/IN2018_V06_%03d/data/MRITC_IMG_IN2018_V06_%03d.CSV' % (
                row.name, row.name)],
            'file_dep': ['/OSM/HBA/OA_BENTHICVIDEO/archive/images/IN2018_V06/sensor_data/image_xif2no.json'],
            'uptodate': [True, ],
            'clean': True,
        }


# %%
from doit import get_var




def task_plot_track():
    def plot_track(item,dependencies,targets):
        try:

            track1 = pd.read_csv(list(dependencies)[0],
                index_col=['timestamp'], parse_dates=['timestamp'])
            track1['x']=1
            track1['x'] = track1['x'].cumsum()
            # Create a new figure, plot into it, then close it so it never gets displayed
            fig, ax = plt.subplots(figsize=(8, 8))
            if 'IMU_pitch' in track1.columns:
                plt.plot(track1[0:200]['x'],track1[0:200]['IMU_pitch'],label='fluentd')
            if 'LAB_Pitch' in track1.columns:
                plt.plot(track1[0:200]['x'],track1[0:200]['LAB_Pitch'],label='labview')
            ax.legend({'fluentd', 'labview'})
            shift =0
            if 'shift' in track1.columns:
                shift = max(track1['shift'])
            ax.set_title('Operation %d shift %f' % (item.name, shift))


        except  FileNotFoundError:
            print('source files missing')
        finally:
            plt.savefig(list(targets)[0])
            plt.close(fig)
        return True

    os.makedirs(basepath+'/plots/labview', exist_ok=True)
    for ind, item in operations.iterrows():
        yield {
            'name': str(ind),
            'actions': [(plot_track, [], {'item': item})],
            # 'task' keyword arg is added automatically
            'targets': [
                basepath+'/plots/labview/MRITC_LAB_IN2018_V06_%03d.jpg' % (
                    item.name)],
            'file_dep': [
                basepath+'/IN2018_V06_%03d/data/MRITC_TRK_IN2018_V06_%03d.CSV' % (
                item.name, item.name)],
            'uptodate': [True, ],
            'clean': True,
        }


# %%
# def task_plot_op():
#     def plot_op(item):
#         try:
#             track = pd.read_csv(
#                 basepath+'/IN2018_V06_%03d/data/MRITC_PKG_IN2018_V06_%03d.CSV' % (
#                 item.name, item.name),
#                 index_col=['timestamp'], parse_dates=['timestamp'])
#             usbl = pd.read_csv(
#                 basepath+'/IN2018_V06_%03d/data/MRITC_USB_IN2018_V06_%03d.CSV' % (
#                 item.name, item.name),
#                 index_col=['timestamp'], parse_dates=['timestamp'])
#             pics = pd.read_csv(
#                 basepath+'/IN2018_V06_%03d/data/MRITC_IMG_IN2018_V06_%03d.CSV' % (
#                 item.name, item.name),
#                 index_col=['timestamp'], parse_dates=['timestamp'])
#             p1 = pd.concat([pics[~pics.index.duplicated(keep='first')], track], sort=False).sort_index()
#             p1 = p1[['UsblEasting', 'UsblNorthing']].interpolate()
#             pics = pics.join(p1)
#             output = basepath+'/plots/op/MRITC_OPR_IN2018_V06_%03d.jpg' % (
#                 item.name)
#             # Create a new figure, plot into it, then close it so it never gets displayed
#             grid = plt.GridSpec(2, 6, wspace=0.5, hspace=0.4)
#             fig = plt.figure(figsize=(10, 8))
#             a0 = fig.add_subplot(grid[0, 0:4])
#             a1 = fig.add_subplot(grid[0, 4]),
#             a2 = fig.add_subplot(grid[0, 5])
#             a3 = fig.add_subplot(grid[1, 0:6])
#             track.plot(x='UsblEasting', y='UsblNorthing', ax=a0)
#             track.plot(x='ShipEasting', y='ShipNorthing', ax=a0)
#             if 'Camera' in pics.columns:
#                 if len(pics.loc[pics.Camera == 'SCS']) > 0:
#                     pics.loc[pics.Camera == 'SCS'].iloc[0::10].plot(x='UsblEasting', y='UsblNorthing', ax=a0,
#                                                                     style='o')
#                     total = pics.groupby('Camera').count()
#                     a0.legend(['raw usbl %d pings' % len(usbl), 'ship',
#                                'images (every 10th port=%d star=%d)' % (total.loc['SCP'][0], total.loc['SCS'][0])])
#             else:
#                 a0.legend(['raw usbl %d pings' % (len(usbl)), 'ship'])
#             a0.axis('equal')
#             a0.set_title("Track Operation %s" % (item.name))
#             (track.pres + track.labAlt).plot(ax=a1)
#             track.pitch.plot(ax=a3)
#             track.roll.plot(ax=a3)
#             track.labAlt.plot(ax=a3)
#             a3.legend(['pitch', 'roll', 'range'])
#             a1.invert_yaxis()
#             track.plot(y='pres', x='temp', ax=a2)
#             a2.invert_yaxis()
#             plt.savefig(output)
#
#         except  FileNotFoundError:
#             print('source files missing')
#         #plt.close(fig)
#         return True
#
#     os.makedirs(basepath+'/plots/op', exist_ok=True)
#     for ind, item in operations.iterrows():
#         yield {
#             'name': str(ind),
#             'actions': [(plot_op, [], {'item': item})],
#             # 'task' keyword arg is added automatically
#             'targets': [
#                 basepath+'/plots/op/MRITC_OPR_IN2018_V06_%03d.jpg' % (
#                     item.name)],
#             'file_dep': [
#                 basepath+'/IN2018_V06_%03d/data/MRITC_PKG_IN2018_V06_%03d.csv' % (
#                 item.name, item.name)],
#             'uptodate': [False, ],
#             'clean': True,
#         }


if __name__ == '__main__':
    import doit

    #print(globals())
    doit.run(globals())

