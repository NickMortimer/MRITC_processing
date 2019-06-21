import pandas as pd
import os
import numpy as np
import subprocess
import cv2
import matplotlib.pyplot as plt
import glob
from imutils import build_montages


# def update_tag(item,picpath):
#     if not os.path.exists(picpath):
#         parts =os.path.split(item.IMG_SourceFile)[1].split('_')
#         parts[5] = item.name.strftime('%Y%m%dT%H%M%SZ')
#         newpath =picpath + 'IN2018_V06_%03d/stills' % (item.Operation) +os.path.sep+'_'.join(parts)
#         NewName =' -o '+newpath
#         os.makedirs(os.path.split(newpath)[0],exist_ok=True)
#         config = '-config /OSM/HBA/OA_BENTHICVIDEO/archive/images/IN2018_V06/sensor_data/exifconfig.txt'
#         file ='/OSM/HBA/OA_IDC/scratch/newRawData/Investigator/in2018_v06/voyage_specific/MRITC/'+item.IMG_SourceFile[2:] + NewName
#         correct_time = '"-alldates=%s" ' %(item.name.strftime('%Y%m%dT%H%M%S.%f'))  #TrackCreateDate
#         custom =  ' -RECORDED_TIMESTAMP="%s" ' %(item.recorded_timestamp.strftime('%Y%m%dT%H%M%S.%f'))
#         gps = '-GPSLongitude="%s"  -GPSLatitude="%s" -GPSAltitudeRef="Below Sea Level" -GPSAltitude="%s" -GPSLongitudeRef="East" -GPSLatitudeRef="South" ' %(item.UsblLongitude,abs(item.UsblLatitude),item.Pres)
#         cmd = 'exiftool '+ config+custom+correct_time+gps+file
#         print(cmd)
#         subprocess.run(cmd,shell=True)#    !{cmd}

def process_video_item(vitem,all,outputs):
    srtfile = outputs['.SRT']
    jsonfile =outputs['.JSON']
    imagefile =outputs['.CSV']                   #datapath +os.path.sep +vitem.FileName.replace('.MP4','.CSV')
    this_vid = pd.DataFrame(all.iloc[(all.index>= vitem.StartTime) & (all.index<= vitem.EndTime)])
    this_vid['video_time'] = this_vid.index - vitem.StartTime
    this_vid['end_time'] =this_vid['video_time'].shift(-1)
    this_vid['end_time'] = this_vid['end_time']  - pd.to_timedelta(5,'ms')
    this_vid['isub']=1
    this_vid['isub'] = this_vid['isub'].cumsum()
    maxtilt = this_vid['Pitch'].min()
    if not np.isnan(maxtilt):
        maxtiltt = this_vid.loc[this_vid['Pitch'].idxmin(),'video_time']
    else:
        maxtiltt=None
    def maketag(time):
        total = time.total_seconds()
        m, s = divmod(total, 60)
        h, m = divmod(m, 60)
        result = "%02d:%02d:%02d,%03d" % (h, m, s, int((total % 1) * 1000))
        return(result)
    isub=1
    FdSub = open(srtfile, 'w')
    FdSub.write("%d\n" % (isub))
    FdSub.write(maketag(pd.Timedelta(0,'s')) + ' --> ' + maketag(pd.Timedelta(30,'s')) + '\n')
    if type(maxtiltt) is pd.Timedelta:
        FdSub.write(" Operation %s minp:%.1f@%s" %(vitem['Operation'],maxtilt,maketag(maxtiltt)[0:-2]) + '\n')
    else:
        FdSub.write(" Operation %s " %(vitem['Operation']) + '\n')

    FdSub.write('\n')

    for index, row in this_vid.iterrows():
        if not ( row['end_time'] is pd.NaT):
            if row['SnapTime']==row['TrueTime']:
                FdSub.write("%d\n" % (isub))
                FdSub.write(maketag(row['video_time']-pd.to_timedelta(1,'ms')) + ' --> ' + maketag(row['video_time']+pd.to_timedelta(1000,'ms')) + '\n')
                FdSub.write(" SNAP %s" %(row['Number']) + '\n')
                FdSub.write('\n')
                isub = isub+1
            delta =(row['TrueTime']-row['SnapTime']).total_seconds()
            if np.isnan(delta):
                delta=0;
            FdSub.write("%d\n" % (isub))
            timestamp =row['TrueTime'].strftime("%Y-%m-%d %H:%M:%S.%f")[0:-5]
            FdSub.write(maketag(row['video_time']) + ' --> ' + maketag(row['end_time']) + '\n')
            FdSub.write("%s %s Lat:%.5f Lon:%.5f D:%.1f Alt:%.1f T:%.2f P:%+-5.1f %s+%d" %
                        (maketag(row['video_time'])[0:-2],timestamp,row['UsblLatitude'],row['UsblLongitude'],
                       row['Pres'],row['Altitude'],row['Temp'],row['Pitch'],row['Number'],delta) + '\n')
            FdSub.write('\n')
            isub = isub+1
    FdSub.close()
    this_vid.index.name='recorded_time'
    json = pd.DataFrame(this_vid[['Temp','Pres', 'Oxy', 'Sal', 'Altitude','UsblLongitude', 'UsblLatitude','depth_meters']])
    json.columns = ['temperature_celsius','pressure_dbar','oxygen_ml_l','salinity','altitude','longitude','latitude','depth_meters']
    json['recorded_timestamp'] = json.index
    if len(json)>0:
        json.to_json(jsonfile,orient='records',date_format='iso',index=True)

def make_path(operation):
    return '/OSM/HBA/OA_BENTHICVIDEO/archive/surveys/IN2018_V06/MRITC/IN2018_V06_%03d/' % (operation)
# load the video data
timing = pd.read_csv(r'/OSM/HBA/OA_BENTHICVIDEO/archive/surveys/IN2018_V06/MRITC/data/timing/timing.csv')
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
videosmp4['OpPath'] = videosmp4.Operation.apply(make_path)
videosmp4 = videosmp4[videosmp4.Operation>10]
plotpath = basepath+'plots/sync/'
os.makedirs(plotpath,exist_ok=True)
#videosmp4.SourceFile =basepath+videosmp4.SourceFile.str[2:]
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
                basename =os.path.split(row.IMG_SourceFile)[1]
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
            parts = ""
            yield {
                'name': str(ind),
                'actions': [(plot_images, [], {'item': item})],
                # 'task' keyword arg is added automatically
                'targets': [plotpath+'MRITC_SVY_IN2018_V06_%03d_%s.JPG'% (item.Operation,parts)],
                'file_dep': [item.SourceFile.replace('.MP4', '.CSV')],

                'uptodate': [True, ],
                'clean': True,
            }



def task_process_video():
    def process_video(item,dependencies,targets):
        outputs =dict()
        for file in list(targets):
            key = os.path.splitext(os.path.basename(file))[1]
            outputs[key] = file
        data = dict()
        for file in list(dependencies):
            key = os.path.basename(file).split('_')[1]
            temp = pd.read_csv(file, index_col=['timestamp'], parse_dates=['timestamp'])
            data[key]=temp
        data['PKG'].index = data['PKG'].index + pd.to_timedelta(item.VideoToPackage, unit='ms')

        if ('IMG' in data.keys()) and (len(data['IMG'])>0) :
            data['IMG'].index = data['IMG'].index + pd.to_timedelta(item.VideoToPicture, unit='ms')
            p2 = data['IMG'].loc[data['IMG']['Camera'] == 'SCS', ['Number', 'Operation','SourceFile']]
            #p2.index = p2.index + pd.to_timedelta(item['VideoToPicture'],unit='ms')
            p2 = p2[(p2.index>=item.StartTime) & (p2.index<=item.EndTime)]
            p2['SnapTime'] = p2.index
            vpackage = pd.concat([data['PKG'], p2], sort=False).sort_index().ffill()
        else :
            vpackage = data['PKG'];
            vpackage['Camera']=''
            vpackage['SnapTime']=vpackage.index[0]
            vpackage['Number'] = np.nan
            vpackage['SourceFile']=''

        vpackage['TrueTime'] = vpackage.index
        #vpackage.index =vpackage.index + pd.to_timedelta(item['VideoToPicture'],unit='ms') -pd.to_timedelta(item['VideoToPackage'],unit='ms')
        process_video_item(item,vpackage,outputs)

        if ('IMG' in data.keys()) and (len(data['IMG'])>0) :

            images =pd.concat([data['IMG'], data['PKG']], sort=False).sort_index()
            images.index.name = 'recorded_time'
            images.Number =images.Number.fillna(method='ffill')
            images.Operation = images.Operation.max()
            images['count'] = images['count'].fillna(method='ffill')
            for col in images.columns:
                if (images[col].dtype == np.float64 or images[col].dtype == np.int64):
                    images[col] = images[col].interpolate()
            #images = images[~images.SourceFile.isna()]
            images = images[(images.index>=(item.StartTime-pd.to_timedelta(2,unit='s'))) & (images.index<=(item.EndTime+pd.to_timedelta(2,unit='s')))]
            up = images.loc[images.index.duplicated(keep='last'), data['PKG'].columns].copy()
            up = up[~up.index.duplicated(keep='first')]
            images.update(up)
            images.to_csv(outputs['.CSV'])




    for ind, item in videosmp4.iterrows():
        yield {
            'name': str(ind),
            'actions': [(process_video, [], {'item': item})],
            # 'task' keyword arg is added automatically
            'targets': [item.SourceFile.replace('.MP4','.SRT'),
                        item.SourceFile.replace('.MP4','.JSON'),
                        item.SourceFile.replace('.MP4', '.CSV')],
            'file_dep': [
                basepath + '/IN2018_V06_%03d/data/MRITC_PKG_IN2018_V06_%03d.CSV' % (item.Operation, item.Operation),
                basepath + '/IN2018_V06_%03d/data/MRITC_IMG_IN2018_V06_%03d.CSV' % (item.Operation, item.Operation)],

            'uptodate': [True, ],
            'clean': True,
        }


def task_make_tag():
    def preprocess_images(item, dependencies, targets):
        data = pd.concat(pd.read_csv(file, index_col=['recorded_time'], parse_dates=['recorded_time']) for file in
                         list(dependencies))
        data.to_csv(list(targets)[0])
        data.resample('1S').last().to_csv(list(targets)[1])

    groups = videosmp4.groupby('Operation')
    for name, group in groups:
        input = []
        for ind, item in group.iterrows():
            input.append(item.SourceFile.replace('.MP4', '.CSV'))
        input = list(filter(lambda file: os.path.exists(file), input))
        input = list(filter(lambda file: os.path.getsize(file) > 2000, input))
        if len(input) > 0:
            yield {
                'name': str(item.Operation),
                'actions': [(preprocess_images, [], {'item': item})],
                # 'task' keyword arg is added automatically
                'targets': [basepath + '/IN2018_V06_%03d/data/MRITC_TAG_IN2018_V06_%03d.CSV' % (
                item.Operation, item.Operation),basepath + '/IN2018_V06_%03d/data/MRITC_TAG1S_IN2018_V06_%03d.CSV' % (
                item.Operation, item.Operation)],
                'file_dep': input,

                'uptodate': [True, ],
                'clean': True,
            }




# def task_process_tag_1_second():
#     def process_tag(dependencies,targets):
#         data = pd.concat(pd.read_csv(file,index_col=['recorded_time'],parse_dates=['recorded_time']) for file in list(dependencies))
#         data.to_csv(list(targets)[0])
#     file_dep =  glob.glob(basepath+'I**/video/MRITC_SVY_*.CSV',recursive=True)
#     return {
#         'actions': [(process_tag, [], )],
#         # 'task' keyword arg is added automatically
#         'targets': [surveypath+'data/output/MRITC_SVY1S_IN2018_V06.csv'],
#         'file_dep': file_dep,
#         'uptodate': [True, ],
#         'clean': True,
#     }
# def task_process_exif():
#     def process_exif(dependencies,targets):
#         images = pd.read_csv(list(dependencies)[0], index_col=['recorded_time'], parse_dates=['recorded_time'])
#         images['NewFileName'] = list(surveypath + images.SourceFile.apply(lambda x: os.path.split(x)[0][2:]) + os.path.sep +images.NewFileName)
#         images['SourceFile'] = '/OSM/HBA/OA_IDC/scratch/newRawData/Investigator/in2018_v06/voyage_specific/MRITC/' + images.SourceFile.apply(lambda x: x[2:])
#         for ind,row in images.iterrows():
#             if not os.path.exists(row.NewFileName):
#                 file = row.SourceFile+ ' -o ' + row.NewFileName
#                 config = '-config /OSM/HBA/OA_BENTHICVIDEO/archive/images/IN2018_V06/sensor_data/exifconfig.txt'
#                 correct_time = '"-alldates=%s" ' % (ind.strftime('%Y%m%dT%H%M%S.%f'))  # TrackCreateDate
#                 custom = ' -RECORDED_TIMESTAMP="%s" ' % (ind.strftime('%Y%m%dT%H%M%S.%f'))
#                 gps = '-GPSLongitude="%s"  -GPSLatitude="%s" -GPSAltitudeRef="Below Sea Level" -GPSAltitude="%s" -GPSLongitudeRef="East" -GPSLatitudeRef="South" ' % (
#                 row.UsblLongitude, abs(row.UsblLatitude), row.Pres)
#                 cmd = 'exiftool ' + config + custom + correct_time + gps + file
#                 print(cmd)
#                 subprocess.run(cmd, shell=True)
#
#     groups = videosmp4.groupby('Operation')
#     for name,group in groups:
#         tagpath =basepath + 'IN2018_V06_%03d/data/MRITC_TAG_IN2018_V06_%03d.CSV' % (name, name)
#         if os.path.exists(tagpath):
#             images = pd.read_csv(tagpath, index_col=['recorded_time'], parse_dates=['recorded_time'])
#             images.drop_duplicates(subset=['NewFileName'],inplace=True)
#             targets =list(basepath + images.SourceFile.apply(lambda x: os.path.split(x)[0][2:])+os.path.sep+images.NewFileName)
#             if len(targets)>0:
#                 yield {
#                     'name': str(name),
#                     'actions': [(process_exif, [], )],
#                     # 'task' keyword arg is added automatically
#                     'targets': targets,
#                     'file_dep': [tagpath],
#
#                     'uptodate': [True, ],
#                     'clean': True,
#                 }





def task_concat_tag_1s():
    def process_tag(dependencies,targets):
        data = pd.concat(pd.read_csv(file,index_col=['recorded_time'],parse_dates=['recorded_time']) for file in list(dependencies))
        data.sort_index(inplace=True)
        data.to_csv(list(targets)[0])
    file_dep =  glob.glob(basepath+'I**/data/*TAG1S*.CSV',recursive=True)
    return {
        'actions': [(process_tag, [], )],
        # 'task' keyword arg is added automatically
        'targets': [basepath+'data/output/MRITC_TAG1S_IN2018_V06.csv'],
        'file_dep': file_dep,
        'uptodate': [True, ],
        'clean': True,
    }

if __name__ == '__main__':
    import doit

    #print(globals())
    doit.run(globals())