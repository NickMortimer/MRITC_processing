import pandas as pd
import os
import numpy as np
basepath = '/OSM/HBA/OA_BENTHICVIDEO/archive/surveys/IN2018_V06/MRITC/'
videosmp4 = pd.read_json('/OSM/HBA/OA_BENTHICVIDEO/archive/surveys/IN2018_V06/MRITC/data/input/video.json')
videosmp4['StartTime']=pd.to_datetime(videosmp4.CreateDate,format='%Y:%m:%d %H:%M:%S')
videosmp4['TrackDuration']=pd.to_timedelta(videosmp4['TrackDuration'])
videosmp4['EndTime'] = videosmp4['StartTime'] + videosmp4['TrackDuration']
videosmp4.set_index('StartTime',inplace=True)
videosmp4.sort_index(inplace=True)
videosmp4['Operation'] = videosmp4.FileName.str.split('_',expand=True)[4].astype(int)
videosmp4 =videosmp4[['TrackDuration','EndTime','FileName','Operation','SourceFile']]
videosmp4['StartTime'] = videosmp4.index
basepath = '/OSM/HBA/OA_BENTHICVIDEO/archive/surveys/IN2018_V06/MRITC/'
surveypath = '/OSM/HBA/OA_BENTHICVIDEO/archive/surveys/IN2018_V06/MRITC/'
videosmp4 = videosmp4[videosmp4.Operation>10]

def task_preprocess_images():
    def time_diff(df_group):
        df_group['FinalTime']=df_group[df_group['Camera'] == 'SCS']['FinalTime'].min()
        return df_group
    def preprocess_images(item,dependencies,targets):
        data = pd.concat(pd.read_csv(file,index_col=['recorded_time'],parse_dates=['recorded_time']) for file in list(dependencies))
        data['count'] =data.groupby(['Operation', 'Number'])['SourceFile'].transform('count')
        data['FinalTime'] = data.index
        if ('SCS' in data.Camera.values):
            data['FinalTime'] = data.groupby(['Operation', 'Number']).transform('min')['FinalTime']
            if (min(abs((data[data.Camera == 'SCS']['FinalTime'] - data[data.Camera == 'SCS'].index)/ np.timedelta64(1, 's')))>0.0001):
                data['FinalTime'] = data.index
                data['FinalTime'] = data.groupby(['Operation', 'Number']).transform('max')['FinalTime']
        data.index =  data['FinalTime']
        copyfields =['Roll','Pitch','Altitude','Temp','Pres','Oxy','Sal','UsblLongitude','UsblLatitude',
                     'UsblNorthing','UsblEasting','ShipNorthing','ShipEasting']
        data.index.name = 'recorded_time'
        scs = data.loc[(data.Camera=='SCS') & (data.Name.str.startswith('MRITC')) ,copyfields]
        scs = scs.loc[~scs.index.duplicated(keep='first')]
        data.update(scs)

        mask =data.Name.str.startswith('MRITC')
        temp =data.SourceFile.apply(os.path.basename).str.split('_',expand=True)
        temp.loc[mask,5] = data.loc[mask,'FinalTime'].apply(lambda x : x.strftime('%Y%m%dT%H%M%SZ'))
        temp.loc[~mask,6] = data.loc[~mask,'FinalTime'].apply(lambda x : x.strftime('%Y%m%dT%H%M%SZ'))
        data.loc[mask,'NewFileName']=temp.loc[mask,[0,1,2,3,4,5,6,]].apply(lambda x: '_'.join(x),axis=1)
        data.loc[~mask,'NewFileName']=temp.loc[~mask].apply(lambda x: '_'.join(x),axis=1)
        data =data.fillna(method='ffill')
        data =data.fillna(method='bfill')
        data.to_csv(list(targets)[0])
    groups = videosmp4.groupby('Operation')
    for name,group in groups:
        input =[]
        for ind, item in group.iterrows():
            input.append(item.SourceFile.replace('.MP4', '.CSV'))
        input = list(filter(lambda file: os.path.exists(file), input))
        input = list(filter(lambda file: os.path.getsize(file)>2000, input))
        if len(input)>0:
            yield {
                'name': str(item.Operation),
                'actions': [(preprocess_images, [], {'item': item})],
                # 'task' keyword arg is added automatically
                'targets': [basepath + '/IN2018_V06_%03d/data/MRITC_TAG_IN2018_V06_%03d.CSV'% (item.Operation,item.Operation)],
                'file_dep': input,

                'uptodate': [False, ],
                'clean': True,
            }

if __name__ == '__main__':
    import doit

    #print(globals())
    doit.run(globals())