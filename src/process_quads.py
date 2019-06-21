import pandas as pd
import os
import numpy as np
import subprocess
import cv2
import matplotlib.pyplot as plt
import glob
from imutils import build_montages


operations = pd.read_csv('/OSM/HBA/OA_BENTHICVIDEO/archive/images/IN2018_V06/sensor_data/operations.csv',
                         index_col='Operation', parse_dates=['StartTime', 'EndTime', 'Duration'])


def make_path(operation):
    return '/OSM/HBA/OA_BENTHICVIDEO/archive/video/IN2018_V06/MRITC/IN2018_V06_%03d/' % (operation)
# load the video data
timing = pd.read_csv(r'/OSM/HBA/OA_BENTHICVIDEO/archive/video/IN2018_V06/MRITC/data/timing/timing.csv')
videosmp4 = pd.read_json('/OSM/HBA/OA_BENTHICVIDEO/archive/images/IN2018_V06/sensor_data/videomp4.json')
videosmp4['StartTime']=pd.to_datetime(videosmp4.CreateDate,format='%Y:%m:%d %H:%M:%S')
videosmp4['TrackDuration']=pd.to_timedelta(videosmp4['TrackDuration'])
videosmp4['EndTime'] = videosmp4['StartTime'] + videosmp4['TrackDuration']
videosmp4.set_index('StartTime',inplace=True)
videosmp4.sort_index(inplace=True)
videosmp4['Operation'] = videosmp4.FileName.str.split('_',expand=True)[4].astype(int)
videosmp4 =videosmp4[['TrackDuration','EndTime','FileName','Operation','SourceFile']]
videosmp4['StartTime'] = videosmp4.index
basepath = '/OSM/HBA/OA_BENTHICVIDEO/archive/video/IN2018_V06/MRITC/'
surveypath = '/OSM/HBA/OA_BENTHICVIDEO/archive/surveys/IN2018_V06/MRITC/'
videosmp4['OpPath'] = videosmp4.Operation.apply(make_path)
videosmp4 = videosmp4[videosmp4.Operation>10]
plotpath = basepath+'plots/sync/'
os.makedirs(plotpath,exist_ok=True)
videosmp4.SourceFile =basepath+videosmp4.SourceFile.str[2:]
timing.index =timing.FileName
videosmp4.index = videosmp4.FileName
videosmp4 = videosmp4.join(timing,rsuffix='Timing')
images =pd.read_csv(surveypath+'data/output/MRITC_TAG_IN2018_V06.csv',index_col=['recorded_time'],parse_dates=['recorded_time'])
images.index =images.SourceFile.apply(os.path.basename).str.split('_',expand=True)[5]

def label_image(img,text):
    font                   = cv2.FONT_HERSHEY_SIMPLEX
    bottomLeftCornerOfText = (10,1070)
    fontScale              = 1
    fontColor              = (255,255,255)
    lineType               = 2

    cv2.putText(img,text,
        bottomLeftCornerOfText,
        font,
        fontScale,
        fontColor,
        lineType)
    return img


def task_process_quads():
    def process_tag(dependencies,targets):
        data = pd.concat(pd.read_csv(file,skiprows=4) for file in list(dependencies))
        data.index = data.Filename.str.split('_', expand=True)[6]
        data = data.join(images[images.Camera=='SCS'],sort=True)
        data.to_csv(list(targets)[0])
    quads =  glob.glob('/datasets/work/OA_SEAMOUNTS_SOURCE/IN2018_V06/IMP/I**/QUAD/Quadrat.csv',recursive=True)
    return {
        'actions': [(process_tag, [], )],
        # 'task' keyword arg is added automatically
        'targets': [surveypath+'data/output/MRITC_QUAD_IN2018_V06.csv'],
        'file_dep': quads,
        'uptodate': [True, ],
        'clean': True,
    }

def task_process_quads_exif():
    def process_tag(item,dependencies,targets):
        if not os.path.exists(list(targets)[0]):
            imagepath = os.path.join(os.path.split(surveypath + item.SourceFile[2:])[0], item.NewFileName)
            config = '-config /OSM/HBA/OA_BENTHICVIDEO/archive/images/IN2018_V06/sensor_data/exifconfig.txt'
            cmd = 'exiftool %s -tagsfromfile %s "-all:all>all" %s -o %s' % (config,imagepath,item.QuadFilePath.replace('.JPG','.jpg'),list(targets)[0])
            subprocess.run(cmd, shell=True)
    quadsfiles =  glob.glob('/datasets/work/OA_SEAMOUNTS_SOURCE/IN2018_V06/IMP/I**/QUAD/Quadrat.csv',recursive=True)
    for file in quadsfiles:
        quads = pd.read_csv(file,skiprows=4)
        quads.index = quads.Filename.str.split('_', expand=True)[6]
        quads = quads.join(images[images.Camera=='SCS'],sort=True)
        quads = quads[~quads.index.duplicated(keep='first')]
        quads = quads[~quads.Operation.isna()]
        quads['QuadFilePath'] = os.path.split(file)[0]+os.path.sep+quads['Filename']
        quads['QuadFilePath'] =  quads['QuadFilePath'].apply(lambda x: os.path.splitext(x)[0])+'_0_Q.jpg'
        outputs =[]
        for ind,row in quads.iterrows():
            quadpath = surveypath + 'IN2018_V06_%03d/quad'% (row.Operation)
            quadpath=quadpath.replace('.CSV', '.csv')
            os.makedirs(quadpath,exist_ok=True)
            quadpath = quadpath +os.path.sep+os.path.basename(row.QuadFilePath)
            yield{
                'name': file+ind,
                'actions': [(process_tag, [],{'item':row} )],
                # 'task' keyword arg is added automatically
                'targets': [quadpath],
                'file_dep': [file],
                'uptodate': [True, ],
                'clean': True,
            }

if __name__ == '__main__':
    import doit

    #print(globals())
    doit.run(globals())