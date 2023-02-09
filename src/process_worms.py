import doit
from doit import get_var
from doit.tools import run_once
from doit import create_after
import glob
from matplotlib.pyplot import axes
import yaml
import os
import pandas as pd
import shutil
import config
from config import task_read_config
import re
import cv2
from skimage import exposure
import xarray as xr
import numpy as np
from pyproj import Proj, transform, Geod

def task_check_concepts():

        def process_list(dependencies, targets):
            concepts =[]
            for file in dependencies:
                data = pd.read_csv(file)
                data['WORMS.concept']=data['concept.substrate']+ data['concept.relief']
                concepts.append({'file':file,'keys':data['WORMS.concept'].unique()})
            data = pd.DataFrame(concepts)
            data.to_csv(targets[0])

        file_dep = glob.glob(config.cfg['paths']['wormssource'])
        target = os.path.join(config.basepath,config.cfg['paths']['process'],'report.csv')
        return {
                'file_dep':file_dep,
                'actions':[process_list],
                'targets':[target],
                'uptodate':[True],
                'clean':True,
        }



def task_summary():
        def sumfile(filename):
            data = pd.read_csv(filename,parse_dates=['rec_time_tag1s'])
            maxd =data.iloc[0]
            mind = data.iloc[-1]
            opnumber = mind.camera_deployment_id.split('_')[-1]
            geod = Geod(ellps='WGS84')
            angle1,angle2,dist1 = geod.inv(maxd.longitude, maxd.latitude, mind.longitude, mind.latitude )
            return { 'Op':opnumber,
                     'Start_Time':mind.rec_time_tag1s,'End_Time':maxd.rec_time_tag1s,
                     'Start_Depth':mind.depth_meters,'End_Depth':maxd.depth_meters,
                     'Start_Latitude':mind.latitude,'End_Latitude':maxd.latitude,
                     'Start_Longitude':mind.longitude,'End_Longitude':maxd.longitude,
                     'distance':dist1
             }

        def process_files(dependencies, targets):
            result = pd.DataFrame([sumfile(file) for file in dependencies])
            result.to_csv(targets[0])

        file_dep = glob.glob(os.path.join(config.basepath,config.cfg['paths']['process'],'worms','*WORMS_concept.CSV'))
        target = os.path.join(config.basepath,config.cfg['paths']['process'],'worms','summary.CSV')
        return {
  
                'file_dep':file_dep,
                'actions':[process_files],
                'targets':[target],
                'uptodate':[True],
                'clean':True,
        }


def task_list_concepts():

        def process_list(dependencies, targets):
            data = pd.concat([pd.read_csv(file) for file in dependencies])
            data['WORMS.concept']=data['concept.substrate']+ data['concept.relief']
            concept=data['WORMS.concept'].unique()
            pd.DataFrame(concept,columns=['concept']).to_csv(targets[0],index=False)

        file_dep = glob.glob(config.cfg['paths']['wormssource'])
        target = os.path.join(config.basepath,config.cfg['paths']['process'],'concepts.csv')
        return {
                'file_dep':file_dep,
                'actions':[process_list],
                'targets':[target],
                'uptodate':[True],
                'clean':True,
          }

def task_process_worms():

        def process_worm(dependencies, targets):
            source_file = list(filter(lambda x: '_worms' in x, dependencies))[0]
            concept_file = list(filter(lambda x: 'final' in x, dependencies))[0]
            concept = pd.read_csv(concept_file)
            data = pd.read_csv(source_file)
            keep =['camera_deployment_id', 'latitude', 'longitude', 'depth_meters',
                    'altitude', 'video_name', 'index_recorded_timestamp', 'rec_time_tag1s',
                    'concept.substrate', 'concept.relief']
            data = data[keep]
            data['concept']=data['concept.substrate']+ data['concept.relief']
            data =data.join(concept.set_index('concept'),on='concept')
            data =data.drop(['concept.substrate', 'concept.relief','concept'],axis=1)
            data.to_csv(targets[0],index=False)

            

        concept = os.path.join(config.basepath,config.cfg['paths']['process'],'concepts_final.csv')
        outdir =os.path.join(config.basepath,config.cfg['paths']['process'],'worms')
        os.makedirs(outdir,exist_ok=True)
        file_dep = glob.glob(config.cfg['paths']['wormssource'],recursive=True)
        for file in file_dep:
            target = os.path.join(outdir,os.path.basename(file)).replace('_worms','_WORMS_concept')
            yield {
                'name' : file,
                'file_dep':[file,concept],
                'actions':[process_worm],
                'targets':[target],
                'uptodate':[True],
                'clean':True,
            }

       

if __name__ == '__main__':
    import doit
    DOIT_CONFIG = {'check_file_uptodate': 'timestamp'}
    #print(globals())
    doit.run(globals())        