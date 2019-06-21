import pandas as pd
import os
import numpy as np
import subprocess
import cv2
import matplotlib.pyplot as plt
import glob
from imutils import build_montages


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
videosmp4['OpPath'] = videosmp4.Operation.apply(make_path)
videosmp4 = videosmp4[videosmp4.Operation>10]
plotpath = basepath+'plots/sync/'
os.makedirs(plotpath,exist_ok=True)
videosmp4.SourceFile =basepath+videosmp4.SourceFile.str[2:]
timing.index =timing.FileName
videosmp4.index = videosmp4.FileName
videosmp4 = videosmp4.join(timing,rsuffix='Timing')

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

def task_plot_images():
    def plot_images(item,dependencies,targets):
        outputs =dict()
        for file in list(targets):
            key = os.path.basename(file).split('_')[1]
            outputs[key] = file

        data = pd.read_csv(list(dependencies)[0],index_col=['recorded_time'],parse_dates=['recorded_time'])
        if len(data)>1:
            data['frame']=(data.index -item.StartTime).total_seconds()*25
            sample =data.sample(6).sort_index()

            videofile = (list(dependencies)[0]).replace( '.CSV','.MP4')
            cap = cv2.VideoCapture(videofile)
            frames = []
            for ind,row in sample.iterrows():

                cap.set(cv2.CAP_PROP_POS_FRAMES, row.frame)
                ret, frame = cap.read()
                frame = label_image(frame, str(ind))
                frames.append(frame)
                basename =os.path.split(row.SourceFile)[1]
                basename = basename.split('_')
                basename[1] = '*'
                basename[5] ='*'
                basename = '_'.join(basename)
                files = glob.glob(os.path.join(os.path.join(item.OpPath,'stills'),basename))
                files.sort()
                for file in files:
                    if len(files)>0:
                        img = cv2.imread(file)
                        img = cv2.resize(img, (1920, 1080))
                        img = label_image(img,os.path.basename(file))
                    else:
                        img = np.zeros((1920, 1080, 3), np.uint8)
                    frames.append(img)
            montages = build_montages(frames, (1920, 1080), (3, 6))
            for montage in montages:
                cv2.imwrite(outputs['SVY'],montage,[int(cv2.IMWRITE_JPEG_QUALITY), 90])
    for ind, item in videosmp4.iterrows():
        parts = os.path.splitext(os.path.basename(item.SourceFile))[0].split('_')[5]
        yield {
            'name': str(ind),
            'actions': [(plot_images, [], {'item': item})],
            # 'task' keyword arg is added automatically
            'targets': [plotpath+'MRITC_SVY_IN2018_V06_%03d_%s.JPG'% (item.Operation,parts)],
            'file_dep': [item.SourceFile.replace('.MP4', '.CSV')],
            'uptodate': [True, ],
            'clean': True,
        }




if __name__ == '__main__':
    import doit

    #print(globals())
    doit.run(globals())