#!/usr/bin/env python
# coding: utf-8

# # Starting glider data analysis
from io import StringIO
from shapely.geometry import LineString
from glob import glob
from matplotlib import style
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import geopandas as gpd
import datetime
import subprocess
import pathlib
import logging
import os
import tqdm
import json 

_log = logging.getLogger(__name__)
if __name__ == '__main__':
    logf = 'cmdconsole_processing.log'
    logging.basicConfig(filename=logf,
                        filemode='a',
                        format='%(asctime)s %(levelname)-8s %(message)s',
                        level=logging.WARNING,
                        datefmt='%Y-%m-%d %H:%M:%S')
    _log.warning("Retreiving command console data")


    #Specify location of the command console data and other necessary files
    loc = '/mnt/samba/Other/glimpse-data/'
    sender = "/home/chiara/pilot_scripts/send_mail.sh"
    mission_WP = json.load(open('/home/chiara/pilot_scripts/mission_wp.json'))
    mails = open('/home/chiara/pilot_scripts/mail_list.txt').read().split(",") 
    

    # Enter every folder (each folder is a glider) in this directory and open the folder with the highest number aka latest mission available for that glider.
    # Once in the lastes mission check if the g-log folder exixts and if it does, then check if there data in the last 24h or not. If there is, then we consider that an active mission and we want to analyse it
    active_mission = []
    for gli in glob(f"{loc}*", recursive=True):
        gli_missions = glob(f"{gli}/*", recursive=True)
        max_mission = max(gli_missions)
        log_data = list(pathlib.Path(f'{max_mission}/G-Logs').glob('*.com.raw.log'))
        if len(log_data) == 0:  # In case the g-log does not exist (rsync from Alseamar was rather recent)
            continue
        cmd_data = pd.read_csv(log_data[0], sep=";", header=0)
        cmd_data.DATE_TIME = pd.to_datetime(cmd_data.DATE_TIME, dayfirst=True, yearfirst=False, )
        latest = cmd_data.where(cmd_data.DATE_TIME > datetime.datetime.now() - datetime.timedelta(hours=24)).dropna()
        if len(latest) > 0:
            active_mission.append(log_data[0])
            
    # For each active mission we create a pandas dataframe 
    def load_cmd(path):
        df = pd.read_csv(path, sep=";", header=0)
        a = df['LOG_MSG'].str.split(',', expand=True)
        cmd = pd.concat([df, a], axis=1)

        # Transform time from object to datetime
        cmd.DATE_TIME = pd.to_datetime(cmd.DATE_TIME, dayfirst=True, yearfirst=False, )
        # Add cycle
        cmd['cycle'] = cmd.where(cmd[0] == '$SEAMRS').dropna(how='all')[3]
        # create lat lon columns in decimal degrees
        cmd['lat'] = cmd.where(cmd[0] == '$SEAMRS').dropna(how='all')[8].str.rsplit('*').str[0]
        cmd['lon'] = cmd.where(cmd[0] == '$SEAMRS').dropna(how='all')[9].str.rsplit('*').str[0]
        cmd['lat'] = cmd.where(cmd[0] == '$SEAMRS').dropna(how='all').lat.replace('', np.nan).dropna(how='all').astype(float)
        cmd['lon'] = cmd.where(cmd[0] == '$SEAMRS').dropna(how='all').lon.replace('', np.nan).dropna(how='all').astype(float)

        def dd_coord(x):
            degrees = x // 100
            minutes = x - 100 * degrees
            res = degrees + minutes / 60
            return res

        df_glider = pd.DataFrame({"time": cmd.dropna(subset=['lon', 'lat']).DATE_TIME,
                                  "lon": dd_coord(cmd['lon'].dropna().astype(float).values),
                                  "lat": dd_coord(cmd['lat'].dropna().astype(float).values),
                                  "cycle": cmd.dropna(subset=['lon', 'lat']).cycle})

        return df_glider

    # Define which transect based on average location
    def find_area(ds):
        area = []
        if ds.lon.mean() < 14:
            area = 'SAMBA_01'
        if ds.lat.mean() < 56:
            area = 'SAMBA_02'
        if ds.lat.mean() > 56 and ds.lat.mean() < 60:
            if ds.lon.mean() > 19:
                area = 'SAMBA_03'
            if ds.lon.mean() > 16 and ds.lon.mean() < 19:
                area = 'SAMBA_04'
        if ds.lat.mean() > 60:
            area = 'SAMBA_05'
        return area

    def find_if_on_transect(ds, buff_lim=1500, time_lim=40):
        st_area = find_area(ds)
        lineStringObj = LineString(list(zip(mission_WP[st_area]['lon'], mission_WP[st_area]['lat'])))
        df_tra = pd.DataFrame()
        df_tra['LineID'] = [101, ]
        line_tran = gpd.GeoDataFrame(df_tra, crs='epsg:4326', geometry=[lineStringObj, ]).to_crs('epsg:3006').buffer(buff_lim)
        buffer_df = gpd.GeoDataFrame(geometry=line_tran).to_crs('epsg:3006')

        sub_glider = ds.where(ds.time > datetime.datetime.now() - datetime.timedelta(hours=time_lim)).dropna()
        sub_mean = sub_glider.groupby('cycle').mean()
        geo_glider = gpd.GeoDataFrame(sub_mean, geometry=gpd.points_from_xy(sub_mean.lon, sub_mean.lat))
        geo_glider = geo_glider.set_crs(epsg=4326).to_crs('epsg:3006')

        polygons_contains = gpd.sjoin(buffer_df, geo_glider, predicate='contains')

        cycle_on = polygons_contains.index_right
        all_cycle = sub_glider.cycle.unique()
        distance = geo_glider.geometry.apply(lambda g: buffer_df.distance(g))
        cycles_off = all_cycle[np.where(np.isin(all_cycle, cycle_on) == False)]
        return cycles_off, distance

    _log.warning("Analysing command console data")
    tab = pd.DataFrame(columns = ['glider','cycles_off', 'area', 'distance'])
    tab.glider = range(0,len(active_mission))

    for i in tqdm.tqdm(range(len(active_mission))):
        act1 = load_cmd(active_mission[i])
        glid_off, dist_tra = find_if_on_transect(act1, buff_lim=1500, time_lim=8)
        if len(glid_off) != 0:
            tab.glider[i] = str(active_mission[i])[:-12][-9:]
            tab.cycles_off[i] = glid_off
            tab.area[i] = find_area(act1)
            tab.distance[i] = np.round(dist_tra.where(dist_tra != 0).dropna(), 0)
    off_glider = tab.dropna()

    final_text = []
    if len(off_glider) !=0:

        for i, row in off_glider.iterrows():
            message = f"The glider SEA{row.glider[3:6]}_M{row.glider[7:10]} is off the transect at dives {row.cycles_off} at a distance {str(row.distance.values.flatten())} m "
            final_text.append(message)

    text = '\n'.join(final_text)

    if len(final_text) != 0:
        for m in mails:
            subprocess.check_call(['/usr/bin/bash', sender, text, "Glider-transect-alert", m])
        
    _log.warning("End analysis - email sent if needed")


