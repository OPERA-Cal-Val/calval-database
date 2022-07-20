#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 19 21:21:49 2022

@author: mbonnema
"""
import geopandas as gpd
import pandas as pd
import boto3
import os
from datetime import datetime

def addImageCalc(filePaths,metaData,awsSession):
    now = datetime.now().strftime("%Y%m%d_%H%M%S")
    s3 = awsSession.resource('s3')
    s3_client = awsSession.client('s3')
    
    print('Creating geojson table')
    s3_key_imagecalc = 'pending/uploads/'+metaData['image_calc_name']+filePaths['image_calc'].split('/')[-1]
    if filePaths['logfile'] != None:
        s3_key_logfile = 'pending/uploads/'+metaData['image_calc_name']+filePaths['logfile'].split('/')[-1]
    else:
        s3_key_logfile = None
    if filePaths['training'] != None:
        s3_key_training = 'pending/uploads/'+metaData['image_calc_name']+filePaths['training'].split('/')[-1]
    else:
        s3_key_training = None
    bucket_name = 'opera-calval-database-dswx'
    bucket_name_staging = 'opera-calval-database-dswx-staging'
    metaData['bucket'] = bucket_name_staging
    metaData['s3_key_imagecalc'] = s3_key_imagecalc
    metaData['s3_key_logfile'] = s3_key_logfile
    metaData['s3_key_training'] = s3_key_training
    metaData['upload_date'] = now
    
    newRow = pd.DataFrame(metaData,index=[0])
    newRow = gpd.GeoDataFrame(newRow,geometry='geometry')
    
    print('Uploading geojson table')
    newRow_bytes = bytes(newRow.to_json(drop_id=True).encode('UTF-8'))
    s3object = s3.Object(bucket_name_staging,'pending/'+now+'_imagecalc.geojson')
    s3object.put(Body=newRow_bytes)
    
    print('Uploading imagecalc')
    response = s3_client.upload_file(filePaths['image_calc'],bucket_name_staging,s3_key_imagecalc)
    if s3_key_logfile != None:
        print('Uploading logfile')
        response = s3_client.upload_file(filePaths['logfile'],bucket_name_staging,s3_key_logfile)
    if s3_key_training != None:
        print('Uploading trainging data')
        response = s3_client.upload_file(filePaths['training'],bucket_name_staging,s3_key_training)