import doit
import pandas as pd
import os

basepath = '/OSM/HBA/OA_BENTHICVIDEO/archive/surveys/IN2018_V06/MRITC/data/input/'

def task_make_json():
    target = basepath+'images.json'
    return {
            'actions': [ 'exiftool /OSM/HBA/OA_BENTHICVIDEO/archive/surveys/IN2018_V06/MRITC/ -RecordedTimestamp -SubSecCreateDate -GPSLatitude -GPSLongitude -GPSAltitude -SerialNumber -R -extn jpg -json > %s' % (target)],
            'targets': [target],
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
        images.set_index('timestamp')
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
        images.to_csv(list(targets)[0])
    file_dep = basepath + 'images.json'
    target = basepath+'images.csv'
    return {
            'actions': [ (process_image_json, [], )],
            'file_dep': [file_dep],
            'targets': [target],
            'uptodate': [True, ],
            'clean': True,
    }

if __name__ == '__main__':
    import doit

    #print(globals())
    doit.run(globals())