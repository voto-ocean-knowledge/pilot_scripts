from shapely.geometry import LineString
from glob import glob
import pandas as pd
import numpy as np
import geopandas as gpd
import datetime
import subprocess
import logging
import os
import json
from pathlib import Path
import sys
script_dir = Path(__file__).parent.absolute()
sys.path.append(str(script_dir))
os.chdir(script_dir)
_log = logging.getLogger(__name__)


# Define which transect it is based on average lat/lon
# ds is the pandas dataframe with timestamp, latitude, longitude and the cycle number
def find_area(ds):
    area = []
    for key in mission_WP.keys():
        polygon_geom = LineString(list(zip(mission_WP[key]['lon'], mission_WP[key]['lat'])))
        df_polytra = pd.DataFrame()
        poly_tran = gpd.GeoDataFrame(df_polytra, crs='epsg:4326', geometry=[polygon_geom, ]).to_crs('epsg:3006').buffer(
            5000)
        buffer_poly = gpd.GeoDataFrame(geometry=poly_tran).to_crs('epsg:3006')

        geo_glider = gpd.GeoDataFrame(ds, geometry=gpd.points_from_xy(ds.lon, ds.lat))
        geo_glider = geo_glider.set_crs(epsg=4326).to_crs('epsg:3006')

        polygons_contains = gpd.sjoin(buffer_poly, geo_glider, predicate='contains')

        if len(polygons_contains) != 0:
            area = key
    if len(area) == 0:
        _log.warning("Could not find a corresponding transect")
        subprocess.check_call(['/usr/bin/bash', sender, text, "Glider-transect-alert", m[0]])
    return area


# ds is the pandas dataframe with timestamp, latitude, longitude and the cycle number
def find_if_on_transect(ds, buff_lim=1500, time_lim=40):
    st_area = find_area(ds)
    lineStringObj = LineString(list(zip(mission_WP[st_area]['lon'], mission_WP[st_area]['lat'])))
    df_tra = pd.DataFrame()
    df_tra['LineID'] = [101, ]
    line_tran = gpd.GeoDataFrame(df_tra, crs='epsg:4326', geometry=[lineStringObj, ]).to_crs('epsg:3006').buffer(
        buff_lim)  # this coordinate system is suitable for Sweden
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
    if len(all_cycle) == 0:
        last_c = np.nan
    else:
        last_c = all_cycle[-1]
    return cycles_off, (distance.where(distance != 0).dropna()).astype(int), last_c

# For each active mission we create a pandas dataframe with timestamp, latitude, longitude and the cycle number
# The path has to direct to the command console data
def load_cmd(path):
    df = pd.read_csv(path, sep=";", usecols=range(0, 6), header=0, encoding_errors='ignore')
    a = df['LOG_MSG'].str.split(',', expand=True)
    cmd = pd.concat([df, a], axis=1)

    # Transform time from object to datetime
    cmd.DATE_TIME = pd.to_datetime(cmd.DATE_TIME, dayfirst=True, yearfirst=False, )
    # Add cycle
    cmd['cycle'] = cmd.where(cmd[0] == '$SEAMRS').dropna(how='all')[3]
    # create lat lon columns in decimal degrees
    cmd['lat'] = cmd.where(cmd[0] == '$SEAMRS').dropna(how='all')[8].str.rsplit('*').str[0]
    cmd['lon'] = cmd.where(cmd[0] == '$SEAMRS').dropna(how='all')[9].str.rsplit('*').str[0]
    cmd['lat'] = cmd.where(cmd[0] == '$SEAMRS').dropna(how='all').lat.replace('', np.nan).dropna(how='all').astype(
        float)
    cmd['lon'] = cmd.where(cmd[0] == '$SEAMRS').dropna(how='all').lon.replace('', np.nan).dropna(how='all').astype(
        float)

    # The SEAMRS nmea sentence prints the coordinates in ddmm.mmm so we what to transform them into dd.dddd
    def dd_coord(x):
        degrees = x // 100
        minutes = x - 100 * degrees
        res = degrees + minutes / 60
        return res
    df_glider = pd.DataFrame({"time": cmd.dropna(subset=['lon', 'lat']).DATE_TIME,
                              "lon": dd_coord(cmd.dropna(subset=['lon', 'lat'])['lon'].astype(float).values),
                              "lat": dd_coord(cmd.dropna(subset=['lon', 'lat'])['lat'].astype(float).values),
                              "cycle": cmd.dropna(subset=['lon', 'lat']).cycle.astype(int)
                              })
    return df_glider


if __name__ == '__main__':
    logf = '/home/pipeline/log/glider_transect.log'
    logging.basicConfig(filename=logf,
                        filemode='a',
                        format='%(asctime)s %(levelname)-8s %(message)s',
                        level=logging.INFO,
                        datefmt='%Y-%m-%d %H:%M:%S')
    _log.info("Retrieving command console data")


    #Specify location of the command console data and other necessary files
    loc = '/data/data_raw/nrt/'
    sender = "/home/pipeline/utility_scripts/send.sh"
    mission_WP = json.load(open('mission_wp.json'))
    mails = open('mail_list.txt').read().split(",")
    if Path("glider_last_alarm.csv").exists():
        cols = list(pd.read_csv("glider_last_alarm.csv").columns)
        glider_last_alarm = pd.read_csv("glider_last_alarm.csv", parse_dates=cols)
    else:
        glider_last_alarm = pd.DataFrame()
        glider_last_alarm.loc[0, "SEA000"] = datetime.datetime.now()

    # Enter every folder (each folder is a glider) in this directory and open the folder with the highest number aka latest mission available for that glider.
    # Once in the lastes mission check if the g-log folder exixts and if it does, then check if there data in the last 24h or not. If there is, then we consider that an active mission and we want to analyse it
    
    active_mission = []
    for gli in glob(f"{loc}*", recursive=True):
        _log.info(f"Reading data from {gli}")
        glider_num = gli.split('/')[-1]
        if glider_num in glider_last_alarm.keys():
            if glider_last_alarm[glider_num][0] > datetime.datetime.now() - datetime.timedelta(hours=24):
                _log.warning(f"glider {glider_num} already alarmed at {glider_last_alarm[glider_num][0]}. Skipping")
                continue
        comm_logs = glob(f"{gli}/*/G-Logs/*com*")
        if not comm_logs:
            continue
        log_data = max(comm_logs)
        cmd_data = pd.read_csv(log_data, sep=";", usecols=range(0, 6), header=0, encoding_errors='ignore')
        if "DATE_TIME" in cmd_data.columns:
            cmd_data.DATE_TIME = pd.to_datetime(cmd_data.DATE_TIME, dayfirst=True, yearfirst=False, )
        else:
            cmd_data['DATE_TIME'] = pd.to_datetime(cmd_data['Date'] + "T" + cmd_data['Time'], dayfirst=True)
        latest = cmd_data.where(cmd_data.DATE_TIME > datetime.datetime.now() - datetime.timedelta(hours=24)).dropna()
        if len(latest) > 0:
            active_mission.append(log_data)

    _log.info("Analysing command console data")
    
    tab = pd.DataFrame(columns=['glider','cycles_off', 'area', 'distance (m)', 'latest_cycle'])
    tab['glider'] = active_mission

    for i in range(len(active_mission)):
        act1 = load_cmd(active_mission[i])
        glid_off, dist_tra, lastC = find_if_on_transect(act1, buff_lim=2000, time_lim=12)
        if len(glid_off) != 0:
            tab.loc[i, 'glider'] = str(active_mission[i])[:-12][-9:]
            tab.at[i, 'cycles_off'] = glid_off
            tab.loc[i, 'area'] = find_area(act1)
            tab.loc[i, 'latest_cycle'] = int(lastC)
            tab.at[i, 'distance (m)'] = dist_tra.values.flatten()
    
    #Remove glider if the lastest cycle is not in the column cycles off, ie glider is back on trasnect and there is no need to alert
    tab_glider = tab.dropna()
    for i, row in tab_glider.iterrows():
        try:
            max_off_cycle = int(row.cycles_off)
        except:
            max_off_cycle = int(max(row.cycles_off))
        if row.latest_cycle > max_off_cycle:
            tab_glider.loc[i,'glider'] = np.nan
    off_glider = tab_glider.dropna()
    final_text = []
    if len(off_glider) != 0:
        for i, row in off_glider.iterrows():
            message = f"The glider SEA{row.glider[3:6]}_M{row.glider[7:10]} is off the transect at dives {row.cycles_off} at a distance {str(row['distance (m)'])} m "
            _log.info(message)
            glider_last_alarm.loc[0, f"SEA{row.glider[3:6]}"] = datetime.datetime.now()
            final_text.append(message)

    text = '\n\n'.join(final_text)

    if len(final_text) != 0:
        for m in mails:
            _log.warning(text)
            subprocess.check_call(['/usr/bin/bash', sender, text, "Glider-transect-alert", m])
    glider_last_alarm.to_csv("glider_last_alarm.csv", index=False)
    _log.info("End analysis - email sent if needed")


