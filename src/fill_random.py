import pandas as pd
import shutil
import os

#'SelNo-NS replaced'
surveypath = '/datasets/work/OA_BENTHICVIDEO_WORK/surveys/IN2018_V06/MRITC/'
imagepath = surveypath + 'IN2018_V06_%03d/stills/%s'
samplepath = '/datasets/work/OA_SEAMOUNTS_SOURCE/IN2018_V06/IMP/IN2018_V06_%03d/RANDSPL/%s'
images =pd.read_csv(surveypath+'data/output/MRITC_TAG_IN2018_V06.csv',index_col=['recorded_time'],parse_dates=['recorded_time'])
images.index =images.NewFileName.str.split('_',expand=True)[4]+'_'+images.NewFileName.str.split('_',expand=True)[6].str.split('.',expand=True)[0]
def task_fill_quads():
    def process_fill(dependencies,targets):
        data = pd.read_csv(list(dependencies)[0],index_col=['KEY'])
        data = data.join(images)
        data.index.name='KEY'
        data['SampleName']=data.NewFileName.str.split('_', expand=True)[1].str[2] + data['SelNo_NS'].astype(str).str.zfill(
            3) + '_' + data.NewFileName
        data =data.drop_duplicates(subset='SampleName')
        data['Operation'] =pd.to_numeric(data.NewFileName.str.split('_',expand=True)[4])
        data.loc[data.Copied.isna(),'Copied'] = False
        #let's copy the images...
        for ind,row in data.iterrows():
            print(ind)
            source = imagepath % (row.Operation,row.NewFileName)
            destination = samplepath % (row.Operation,row.SampleName)
            if (not os.path.exists(destination)) & (not row['Copied']):
                print('cp %s --> %s' % (source, destination))
                shutil.copy(source,destination)
                data.at[ind,'Copied']=True
            else:
                data.at[ind,'Copied']=True
        data =data[['SampleName','SelNo_NS','Copied']]
        data.to_csv(list(targets)[0],index=True)

    input =  '/datasets/work/OA_SEAMOUNTS_SOURCE/IN2018_V06/IMP/StillFilling_Input.csv'
    output = '/datasets/work/OA_SEAMOUNTS_SOURCE/IN2018_V06/IMP/StillFilling_Output.csv'
    return {
        'actions': [(process_fill, [], )],
        # 'task' keyword arg is added automatically
        'targets': [output],
        'file_dep': [input],
        'uptodate': [True, ],
        'watch': ['/datasets/work/OA_SEAMOUNTS_SOURCE/IN2018_V06/IMP/'],
        'clean': True,
    }



if __name__ == '__main__':
    import doit

    #print(globals())
    doit.run(globals())