import pandas as pd
import glob
import sqlite3
import os
import numpy as np
import xarray as xr
from pyproj import Proj
from pykalman import KalmanFilter
from scipy.signal import correlate
import numpy as np
from numpy import ma
import gsw
basepath = '/OSM/HBA/OA_BENTHICVIDEO/archive/surveys/IN2018_V06/MRITC/'
databasepath = '/OSM/HBA/OA_BENTHICVIDEO/archive/surveys/IN2018_V06/MRITC/data/'
dataoutputpath = '/OSM/HBA/OA_BENTHICVIDEO/archive/surveys/IN2018_V06/MRITC/data/processed/'
fluentspath = '/OSM/HBA/OA_BENTHICVIDEO/archive/surveys/IN2018_V06/MRITC/data/fluentd/'
navpath = '/OSM/HBA/OA_BENTHICVIDEO/archive/surveys/IN2018_V06/MRITC/data/nav/'
usbl2path = '/OSM/HBA/OA_BENTHICVIDEO/archive/surveys/IN2018_V06/MRITC/data/usbl2/'
labviewpath = '/OSM/HBA/OA_BENTHICVIDEO/archive/surveys/IN2018_V06/MRITC/data/labview/'



def convertlatlong(latitude, longitude, hemi):
    latitude = (latitude // 100 + (latitude % 100) / 60)
    latitude[hemi == 'S'] = latitude[hemi == 'S'] * -1
    longitude = (longitude // 100 + (longitude % 100) / 60)
    return {'latitude': latitude, 'longitude': longitude}

def get_median_filtered(signal, threshold=3):
    signal = signal.copy()
    difference = np.abs(signal - np.median(signal))
    median_difference = np.median(difference)
    if median_difference == 0:
        s = 0
    else:
        s = difference / float(median_difference)
    mask = s > threshold
    signal[mask] = np.median(signal)
    return signal

def processfile(file):
    print(file)
    usbl1 = None
    alt = None
    imu = None
    ctd = None
    data = pd.read_json(file, lines=True)
    data.index = data.timestamp
    if 'instrument' in data.columns:
        data.rename(columns={'instrument': 'message'})
    if 'message' in data.columns:
        temp = data[data.message == 'mritc.datasonics.alt.raw']
        if not temp.empty:
            alt = temp.data.str.extract(r'^.*R(?P<range>[-+]?[0-9]*\.?[0-9]+)', expand=True)
            alt.dropna(inplace=True)

            # alt.to_sql('alt', conn, if_exists="append")
        temp = data[data.message == 'mritc.lord.imu.raw']
        if not temp.empty:
            imu = temp['data'].str.split(',', expand=True).astype(float)
            imu.columns = ['roll', 'pitch']
            imu.dropna(inplace=True)
            # pr.to_sql('atti',conn,if_exists="append")
        temp = data[data.message == 'mritc.seabird.ctd.raw']
        if not temp.empty:
            temp = temp['data'].str.split(',', expand=True).astype(float, errors='ignore')
            if len(temp.columns) == 5:
                ctd = temp
                ctd.columns = ['temp', 'con', 'pres', 'oxy', 'sal']
                for col in ctd.columns:
                    ctd[col] = pd.to_numeric(ctd[col],errors='coerce')
                ctd.dropna(inplace=True)
        temp = data[data.message == 'sonardyn.usbl.raw']
        if not temp.empty:
            usbl = temp['data'].str.split(',', expand=True).astype(float, errors='ignore')
            usbl[2] = usbl[2].astype(float)
            usbl[4] = usbl[4].astype(float)
            usbl[3] = usbl[3].astype(str)
            usbl1 = pd.DataFrame(convertlatlong(usbl[2], usbl[4], usbl[3]))
            usbl1.index = usbl.index
            usbl1['depth'] = usbl[9].astype(float)
            usbl1.dropna(inplace=True)

    return {'IMU': imu, 'CTD': ctd, 'USBL': usbl1, 'ALT': alt}





def task_process_fluentd():
    """ read in all the fluentd json files and process them to csv file
    """
    def save_data(item, path):
        """
        Take the data and save it including a zero length file if there is no data
        :param item: the pands data frame with the data
        :param path: path to store the file as CSV
        :return:
        """
        for key in item.keys():
            for output in path:
                if key in output:
                    if item[key] is not None:
                        item[key].to_csv(output)
                    else:
                        with open(output, 'w'):
                            pass
    def action(dependencies,targets):
        """

        :param dependencies: list of files to process
        :param targets: list of file to output
        :return:
        """
        data = processfile(list(dependencies)[0])
        save_data(data, list(targets))
    os.makedirs(dataoutputpath, exist_ok=True)
    files = glob.glob(fluentspath+'*.log',recursive=True)
    files = files + glob.glob(fluentspath+'.*.log',
                              recursive=True)

    for file in files:
        basename = os.path.splitext(os.path.basename(file))[0]
        targets =[]
        for name in ['IMU','CTD','USBL','ALT']:
            targets.append(dataoutputpath + basename + '_'+name+'.CSV')
        yield {
            'name': str(file),
            'actions': [(action, [], )],
            # 'task' keyword arg is added automatically
            'targets': targets,
            'file_dep': [file],
            'uptodate': [True, ],
            'clean': True,
        }







def task_process_labview_txt():
    def cleanlabview(dependencies,targets):
        data = pd.read_csv(list(dependencies)[0], sep=',', usecols=['PC_Date', 'PC_Time', 'Roll', 'Pitch', 'Ana1', 'Ana2', 'V12',
                                                   'V24', 'Status', 'Depth', 'Altitude', 'Wire_Out', 'CTD_Temp',
                                                   'CTD_Cond', 'CTD_Press', 'CTD_DO', 'CTD_Sal', 'GPS_UTC_Time',
                                                   'Ship_Lat', 'N_S', 'Ship_Long', 'E_W', 'COG', 'SOG',
                                                   'Ship_Heading'], index_col=False)
        data['PcTimeStamp'] = pd.to_datetime(data.PC_Date + ' ' + data.PC_Time, format='%Y-%m-%d %H:%M:%S.%f')
        data['GpsTimeStamp'] = pd.to_datetime(
            data['PC_Date'] + ' ' + data['GPS_UTC_Time'].astype(str).apply(lambda x: x.zfill(9)),
            format='%Y-%m-%d %H%M%S.%f')
        timediff = data.GpsTimeStamp - data.PcTimeStamp
        index = np.argmin(abs(timediff))
        data.index = data.PcTimeStamp + timediff[index]
        data.index.name ='timestamp'
        # data.index =pd.to_datetime(data.PC_Date+' '+data.GPS_UTC_Time,errors='ignore')
        data = data[
            ['Roll', 'Pitch', 'CTD_Press', 'CTD_Temp', 'CTD_Cond', 'CTD_DO', 'CTD_Sal', 'Altitude', 'GpsTimeStamp']]
        for col in ['Roll', 'Pitch', 'CTD_Press', 'CTD_Temp', 'CTD_Cond', 'CTD_DO', 'CTD_Sal', 'Altitude']:
            data[col] = pd.to_numeric(data[col],errors='coerce')
        data.to_csv(list(targets)[0])

    os.makedirs(dataoutputpath, exist_ok=True)
    files = glob.glob(labviewpath+'*.txt')

    for file in files:
        basename = os.path.splitext(os.path.basename(file))[0]
        yield {
            'name': str(file),
            'actions': [(cleanlabview, [], )],
            # 'task' keyword arg is added automatically
            'targets': [os.path.join(dataoutputpath,basename +'_LAB.CSV')],
            'file_dep': [file],
            'uptodate': [True, ],
            'clean': True,
        }



def task_process_techsas_usbl():

    def load_usbl(file):
        # print(file)
        u = pd.read_csv(file, header=None, sep=r",|\s+", engine='python', skip_blank_lines=True)
        u.index = pd.to_datetime(u[0])
        u = u[[6, 7, 8]]
        u = u[[6, 7, 8]]
        u.columns = ('x', 'y', 'z')
        u.index.name = 'timestamp'
        return u
    def process_usbl(dependencies,targets):
        usbl_files = list(filter(lambda file: file.endswith('.usbl2'), list(dependencies)))
        nc_files = list(filter(lambda file: file.endswith('.nav'), list(dependencies)))
        usbl_data = pd.concat([load_usbl(file) for file in usbl_files])
        usbl_data.sort_index(inplace=True)
        # read in all the ships postions
        position =xr.open_mfdataset(nc_files)
        pos =position.to_dataframe()
        ship =pd.concat([pos, usbl_data],sort=False).sort_index()
        ship =ship[['lat','long']].interpolate()
        usbl_data =usbl_data.join(ship)
        myProj = Proj("+proj=utm +zone=55F, +south +ellps=WGS84 +datum=WGS84 +units=m +no_defs")
        usbl_data['ShipEasting'], usbl_data['ShipNorthing'] = myProj(np.array(usbl_data['long']), np.array(usbl_data['lat']))
        usbl_data['UsblEasting']  = usbl_data['ShipEasting'] + usbl_data['x']
        usbl_data['UsblNorthing'] = usbl_data['ShipNorthing'] + usbl_data['y']
        usbl_data['UsblLongitude'], usbl_data['UsblLatitude'] = myProj(np.array(usbl_data['UsblEasting']),
                                                                         np.array(usbl_data['UsblNorthing']),inverse='True')
        usbl_data['dt'] = usbl_data.index
        usbl_data['UsblSpeed']=np.power(np.power(usbl_data.UsblEasting.diff(),2) + np.power(usbl_data.UsblNorthing.diff(),2),0.5)/usbl_data['dt'].diff().dt.total_seconds()
        usbl_data['ShipSpeed']=np.power(np.power(usbl_data.ShipEasting.diff(),2) + np.power(usbl_data.ShipNorthing.diff(),2),0.5)/usbl_data['dt'].diff().dt.total_seconds()
        usbl_data = usbl_data[usbl_data.z>100]
        usbl_data =usbl_data[usbl_data['UsblSpeed']<5]
        usbl_data.index.name ='timestamp'
        usbl_data.to_csv(list(targets)[0])

    files = glob.glob(usbl2path+'*.usbl2') + \
            glob.glob(navpath+'*.nav')
    return {
            'actions': [(process_usbl, [], )],
            # 'task' keyword arg is added automatically
            'targets': [dataoutputpath+'MRITC_SAS_IN2018_V06.CSV'],
            'file_dep': files,
            'uptodate': [True, ],
            'clean': True,
        }

def task_process_techsas_gps():

    def process_gps(dependencies,targets):
        nc_files = list(filter(lambda file: file.endswith('.nav'), list(dependencies)))
        position =xr.open_mfdataset(nc_files).to_dataframe()
        position.index.name = 'timestamp'
        position.to_csv(list(targets)[0])

    files = glob.glob(usbl2path+'*.usbl2') + \
            glob.glob(navpath+'*.nav')
    return {
            'actions': [(process_gps, [], )],
            # 'task' keyword arg is added automatically
            'targets': [dataoutputpath+'MRITC_NAV_IN2018_V06.CSV'],
            'file_dep': files,
            'uptodate': [True, ],
            'clean': True,
        }

def task_process_concat_output():
    def do_imu(dependencies,targets):
        data = pd.concat([pd.read_csv(file,index_col=['timestamp'],parse_dates=['timestamp']) for file in list(dependencies)])
        data.sort_index(inplace=True)
        data.to_csv(list(targets)[0])

    for groups in ['IMU','CTD','USBL','ALT','LAB']:
        files = glob.glob(dataoutputpath+'*_'+groups+'.CSV') + \
                glob.glob(dataoutputpath+'.*_'+groups+'.CSV')
        files = list(filter(lambda file: os.stat(file).st_size > 0, files))
        targ = dataoutputpath+'MRITC_'+groups+'_IN2018_V06.CSV'
        yield { 'name': targ,
                'actions': [(do_imu, [], )],
                # 'task' keyword arg is added automatically
                'targets': [targ],
                'file_dep': files,
                'uptodate': [True, ],
                'clean': True,
            }

def task_process_convert_to_hdf():
    def to_hdf(dependencies,targets):
        store = pd.HDFStore(list(targets)[0])
        for file in list(dependencies):
            key = os.path.basename(file).split('_')[1]
            data =pd.read_csv(file, index_col=['timestamp'], parse_dates=['timestamp'])
            if key=='LAB':
                data.GpsTimeStamp = pd.to_datetime(data.GpsTimeStamp)
            if key=='SAS':
                data.dt = pd.to_datetime(data.dt)
            store[key]=data
        store.close()
    files = glob.glob(dataoutputpath+'MRITC*.CSV')
    files = list(filter(lambda file: os.stat(file).st_size > 0, files))
    targ = dataoutputpath+'MRITC_HDF_IN2018_V06.HDF'
    return {
            'actions': [(to_hdf, [], )],
            # 'task' keyword arg is added automatically
            'targets': [targ],
            'file_dep': files,
            'uptodate': [True, ],
            'clean': True,
        }



operations = pd.read_csv(databasepath+'input/operations.csv',
                         index_col='Operation', parse_dates=['StartTime', 'EndTime', 'Duration'])



def task_cut_data():
    def process_cut_data(dependencies,targets,row):
        for file in targets:
            key = os.path.basename(file).split('_')[1]
            data = pd.read_hdf(list(dependencies)[0], key=key)
            data = data[(data.index > (row.StartTime - pd.to_timedelta('600S'))) & (
                        data.index < (row.EndTime + pd.to_timedelta('600S')))]
            os.makedirs(os.path.dirname(file),exist_ok=True)
            data.to_csv(file)

    file_dep = dataoutputpath+'MRITC_HDF_IN2018_V06.HDF'
    for ind, row in operations.iterrows():
        targets =[]
        for groups in ['IMU', 'CTD', 'USBL', 'ALT', 'LAB','SAS','NAV']:
            targets.append(basepath+'/IN2018_V06_%03d/data/MRITC_%s_IN2018_V06_%03d.CSV' %(ind,groups,ind))
        yield {
            'name': str(ind),
            'actions': [(process_cut_data, [], {'row':row})],
            # 'task' keyword arg is added automatically
            'targets': targets,
            'file_dep': [file_dep],
            'uptodate': [True, ],
            'clean': True,
        }

def interpusblposition(usbl):

    # ok lest make sure the GPS is on the second
    usbl = usbl.drop_duplicates()
    timesteps = np.diff(usbl.index)/np.timedelta64(1, 's')
    Transition_Matrix = np.array([[1, 0, 1, 0], [0, 1, 0, 1], [0, 0, 1, 0], [0, 0, 0, 1]])

    temp = np.zeros((len(timesteps), 4, 4))
    for i in range(len(timesteps)):
        Transition_Matrix[0, 2] = timesteps[i]
        Transition_Matrix[1, 3] = timesteps[i]
        temp[i] = Transition_Matrix
    Transition_Matrix = temp
    mask=usbl['UsblNorthing'][:-1].isna()
    lat = ma.array(usbl['UsblNorthing'][:-1],mask=mask)
    lon = ma.array(usbl['UsblEasting'][:-1],mask=mask)
    #print(timesteps.min())
    lonSpeed = ma.array(np.diff(usbl.ShipEasting)/timesteps)
    lonSpeed[np.isnan(lonSpeed)] =0
    latSpeed = ma.array(np.diff(usbl.ShipNorthing) / timesteps)
    latSpeed[np.isnan(latSpeed)] =0
    coord = ma.dstack((lon, lat,lonSpeed,latSpeed))[0]
    Observation_Matrix = np.eye(4)
    xinit = lon[0]
    yinit = lat[0]
    vxinit =lonSpeed[0]
    vyinit = latSpeed[0]
    initstate = [xinit, yinit, vxinit, vyinit]
    #print(initstate)
    initcovariance = 1.0e-3 * np.eye(4)
    transistionCov = 1.0e-3 * np.eye(4)
    observationCov =np.eye(4)
    observationCov[0,0]  =15
    observationCov[1,1]  =15
    observationCov[2,2]  = 0.01
    observationCov[3,3]  =0.01
    #print('kman')
    kf = KalmanFilter(transition_matrices=Transition_Matrix,
                      observation_matrices=Observation_Matrix,
                      initial_state_mean=initstate,
                      initial_state_covariance=initcovariance,
                      transition_covariance=transistionCov,
                      observation_covariance=observationCov)

    output = kf.smooth(coord)[0]
    result = usbl
    result['KfUsblNorthing'] =np.append(output[:, 1],output[-1, 1])
    result['KfUsblEasting'] =np.append(output[:, 0],output[-1, 0])
    myProj = Proj("+proj=utm +zone=55F, +south +ellps=WGS84 +datum=WGS84 +units=m +no_defs")
    result['KfUsblLongitude'], result['KfUsblLatitude'] = myProj(result['KfUsblEasting'].values,
                                                          result['KfUsblNorthing'].values,
                                                          inverse='True')

    return result

def task_filter_usbl():
    def process_usbl(dependencies,targets):
        usbl = pd.read_csv(list(dependencies)[0],index_col=['timestamp'],parse_dates=['timestamp'])
        print ('Number of pings %s'%(len(usbl)))
        if len(usbl)>10:
            usbl =interpusblposition(usbl)
            usbl.to_csv(list(targets)[0])
    for ind, row in operations.iterrows():
        yield {
            'name': str(ind),
            'actions': [(process_usbl, [], )],
            # 'task' keyword arg is added automatically
            'targets': [basepath+'/IN2018_V06_%03d/data/MRITC_USB_IN2018_V06_%03d.CSV' %(ind,ind)],
            'file_dep': [basepath+'/IN2018_V06_%03d/data/MRITC_SAS_IN2018_V06_%03d.CSV' %(ind,ind)],
            'uptodate': [True, ],
            'clean': True,
        }

def task_write_track():
    def write_track(item, dependencies, targets):
        data = dict()
        for file in list(dependencies):
            key = os.path.basename(file).split('_')[1]
            temp = pd.read_csv(file, index_col=['timestamp'], parse_dates=['timestamp'])
            if len(temp) > 0:
                data[key] = temp
                data[key].columns = key + '_' + data[key].columns
        if 'ALT' in data.keys():
            if data['ALT']['ALT_range'].mean() < 100:
                data['ALT']['ALT_range'] = get_median_filtered(data['ALT']['ALT_range'])
        if 'LAB' in data.keys():
            data['LAB']['LAB_Altitude'] = get_median_filtered(data['LAB']['LAB_Altitude'])
            if ('IMU' in data.keys()):  # correct the time
                try:
                    both = pd.concat([data['IMU'], data['LAB']], sort=True).interpolate()
                    both.index.name = 'timestamp'
                    both = both.sort_index().resample('100L').first()
                    both = both.interpolate()
                    both.dropna(subset=['LAB_Pitch', 'IMU_pitch'], inplace=True)
                    xcorr = correlate(both.IMU_pitch.values, both.LAB_Pitch.values)
                    nsamples = len(both)
                    dt = np.arange(1 - nsamples, nsamples)
                    timedelta = dt[xcorr.argmax()]
                    data['LAB'].index = data['LAB'].index + pd.to_timedelta(timedelta * 100, unit='ms')
                    data['LAB']['shift'] = timedelta * 100
                except ValueError:
                    data['LAB']['shift'] = np.nan
        both = pd.concat(list(data.values()), sort=False).sort_index()
        both.index.name = 'timestamp'
        both = both.sort_index().resample('100L').first()
        both = both.interpolate()
        both.fillna(method='ffill')
        both.fillna(method='bfill')
        both.to_csv(list(targets)[0])

    for ind, item in operations.iterrows():
        dep = []
        for groups in ['IMU', 'CTD', 'ALT', 'LAB', 'USB','NAV']:
            dep.append(
                basepath + 'IN2018_V06_%03d/data/' % (ind) + 'MRITC_' + groups + '_IN2018_V06_%03d.CSV' % (ind))
        dep = list(filter(lambda file: os.path.exists(file), dep))
        yield {
            'name': str(ind),
            'actions': [(write_track, [], {'item': item})],
            # 'task' keyword arg is added automatically
            'targets': [
                basepath + 'IN2018_V06_%03d/data/MRITC_TRK_IN2018_V06_%03d.CSV' % (
                    item.name, item.name)],
            'file_dep': dep,
            'uptodate': [True, ],
            'clean': True,
        }

def get_median_filtered(signal, threshold=3):
    signal = signal.copy()
    difference = np.abs(signal - np.median(signal))
    median_difference = np.median(difference)
    if median_difference == 0:
        s = 0
    else:
        s = difference / float(median_difference)
    mask = s > threshold
    signal[mask] = np.median(signal)
    return signal

def task_write_package():
    def write_package(item, dependencies, targets):
        data = pd.read_csv(list(dependencies)[0], index_col=['timestamp'], parse_dates=['timestamp'])
        if 'ALT_range' in data.columns:
            data = data.rename(columns={'ALT_range': 'Altitude'})
        elif 'LAB_Altitude' in data.columns:
            data = data.rename(columns={'LAB_Altitude': 'Altitude'})
        else:
            data['Altitude'] = np.nan
        if 'IMU_pitch' in data.columns:
            data = data.rename(columns={'IMU_pitch': 'Pitch', 'IMU_roll': 'Roll'})
        elif 'LAB_Pitch' in data.columns:
            data = data.rename(columns={'LAB_Pitch': 'Pitch', 'LAB_Roll': 'Roll'})
        else:
            data = data['Roll'] = np.nan
            data = data['Pitch'] = np.nan
        if 'USB_KfUsblEasting' in data.columns:
            data = data.rename(columns={
                'USB_KfUsblEasting': 'UsblEasting',
                'USB_KfUsblNorthing': 'UsblNorthing',
                'USB_KfUsblLongitude': 'UsblLongitude',
                'USB_KfUsblLatitude': 'UsblLatitude',
                'USB_long': 'ShipLongitude',
                'USB_lat': 'ShipLatitude',
                'USB_ShipNorthing': 'ShipNorthing',
                'USB_ShipEasting': 'ShipEasting'
            })
        else:
            data['UsblEasting'] = np.nan
            data['UsblNorthing'] = np.nan
            data['UsblLongitude'] = np.nan
            data['UsblLatitude'] = np.nan
            data['ShipNorthing'] = np.nan
            data['ShipEasting'] = np.nan
            data['UsblLatitude'] = np.nan
            data['UsblLongitude'] = np.nan
            data['ShipLatitude'] = np.nan
            data['ShipLongitude'] = np.nan
        if  'NAV_lat' in data.columns:
            data['ShipLatitude'] = data['NAV_lat']
            data['ShipLongitude'] = data['NAV_long']
        if 'CTD_pres' in data.columns:
            data = data.rename(columns={'CTD_pres': 'Pres', 'CTD_temp': 'Temp',
                                        'CTD_oxy': 'Oxy', 'CTD_sal': 'Sal'})
        elif 'LAB_CTD_Temp' in data.columns:
            data = data.rename(columns={'LAB_CTD_Press': 'Pres', 'LAB_CTD_Temp': 'Temp',
                                        'LAB_CTD_DO': 'Oxy', 'LAB_CTD_Sal': 'Sal'})
        else:
            data['Oxy'] = np.nan
            data['Sal'] = np.nan
            data['Temp'] = np.nan
            if 'USB_x' in data.columns:
                data = data.rename(columns={'USB_x': 'Pres'})
            else:
                data['Pres'] = np.nan
        cols = ['Roll', 'Pitch', 'Altitude', 'Temp', 'Pres', 'Oxy', 'Sal', 'UsblLongitude', 'UsblLatitude',
                'UsblNorthing', 'UsblEasting', 'ShipNorthing', 'ShipEasting']
        for col in cols:
            data[col] = pd.to_numeric(data[col], errors='coerce')
        data['depth_meters']=np.abs(gsw.z_from_p(data['Pres'].values,data.loc[~data['ShipLatitude'].isna(),'ShipLatitude'].values.max()))
        cols = ['Roll', 'Pitch', 'Altitude', 'Temp', 'Pres', 'Oxy', 'Sal', 'UsblLongitude', 'UsblLatitude',
                'UsblNorthing', 'UsblEasting', 'ShipNorthing', 'ShipEasting','depth_meters',
                'ShipLatitude','ShipLongitude']
        data[cols].to_csv(list(targets)[0])

    for ind, item in operations.iterrows():
        yield {
            'name': str(ind),
            'actions': [(write_package, [], {'item': item})],
            # 'task' keyword arg is added automatically
            'targets': [
                basepath + 'IN2018_V06_%03d/data/MRITC_PKG_IN2018_V06_%03d.CSV' % (
                    item.name, item.name)],
            'file_dep': [basepath + 'IN2018_V06_%03d/data/MRITC_TRK_IN2018_V06_%03d.CSV' % (
                item.name, item.name)],
            'uptodate': [True, ],
            'clean': True,
        }

def task_process_image_json():
    def process_image_json(dependencies,targets):
        images = pd.read_json(list(dependencies)[0])
        images.head()
        images.SubSecCreateDate = pd.to_datetime(images.SubSecCreateDate, format='%Y:%m:%d %H:%M:%S.%f')
        images['timestamp'] = images.SubSecCreateDate
        images.RecordedTimestamp = pd.to_datetime(images.RecordedTimestamp)
        images.set_index('timestamp',inplace=True)
        images.sort_index(inplace=True)
        images['Name'] = images.SourceFile.apply(os.path.basename)
        images['Path'] = images.SourceFile.apply(os.path.basename)
        parts = images['Name'].str.split('_', expand=True)
        mask =images.SourceFile.apply(os.path.dirname).str.endswith('stills')
        images.loc[mask,'Camera'] = parts.loc[mask,1]
        images.loc[mask,'Operation'] = parts.loc[mask,4]
        images.loc[mask,'Number'] = parts.loc[mask,6].str.split('.', expand=True)[0]
        mask1 = images.SourceFile.apply(os.path.dirname).str.endswith('quad')
        images.loc[mask1,'Camera'] = parts.loc[mask1,2]
        images.loc[mask1,'Operation'] = parts.loc[mask1,5]
        images.loc[mask1,'Number'] = parts.loc[mask1,7].str.split('.', expand=True)[0]
        images = images[(mask | mask1)]
        images['Operation'] = images['Operation'].astype(int)
        images.to_csv(list(targets)[0],index=True)
    file_dep = databasepath + 'input/images.json'
    target = databasepath+'input/images.csv'
    return {
            'actions': [ (process_image_json, [], )],
            'file_dep': [file_dep],
            'targets': [target],
            'uptodate': [False, ],
            'clean': True,

    }

images = pd.read_csv(databasepath+'input/images.csv',index_col=['timestamp'],parse_dates=['timestamp'])
def task_write_image_data():
    def write_image(item,targets):
        images.loc[(images.index > item.StartTime) & (images.index < item.EndTime),'Operation'] =item.name
        data = images[(images.index > item.StartTime) & (images.index < item.EndTime)].copy()
        if len(data)>0:
            data['count'] =data.groupby(['Operation', 'Number'])['SourceFile'].transform('count')
            data['Key'] = data['Operation'].astype(str).str.zfill(3)+'_'+data['Number'].astype(str).str.zfill(4)
            data = data.reset_index().set_index('Key')
            data.update(data.loc[(data.Camera == 'SCS') & data.Name.str.startswith('MRITC'),'timestamp'])
            data = data.reset_index().set_index('timestamp')
        data.to_csv(list(targets)[0])

    file_dep = databasepath + 'input/images.csv'
    for ind, row in operations.iterrows():
        yield {
            'name': str(ind),
            'actions': [(write_image, [], {'item': row})],
            # 'task' keyword arg is added automatically
            'targets': [
                basepath+'IN2018_V06_%03d/data/MRITC_IMG_IN2018_V06_%03d.CSV' % (
                row.name, row.name)],
            'file_dep': [file_dep],
            'uptodate': [True, ],
            'clean': True,
        }
if __name__ == '__main__':
    import doit

    #print(globals())
    doit.run(globals())
