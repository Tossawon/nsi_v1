import os
from tempfile import TemporaryFile
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
import pandas as pd
from datetime import datetime
from rasterio.enums import Resampling
import numpy as np
import rasterio as rio


def readLastRowNDWI(NDWIpath):
    NDWIpath = NDWIpath
    NDWIfile = "NDWIList.txt"

    try:
        with open(f"{NDWIpath}/{NDWIfile}", "r") as file:
            last_line = file.readlines()[-1]
            if len(last_line)!= 0:
                data = last_line.split(" ")
                path =data[-1].strip("\n")
                print(path)
                tile = path.split("/")[2]
                date = path.split("/")[3]
                print(tile, date)
                return tile, date
            else:
                print("There is nothing in the NDWIfile.txt")


    except Exception as e:
        print("Exception error during read NDWIlist file")
def updateListNDWI(ti):
    NDWIpath = "/home/airflow/NDWIList"
    bucket = "nsi-satellite-prototype"
    bucket_folder = "sprectral_index"
    command = f"aws s3 ls s3://{bucket}/{bucket_folder}/ --recursive > {NDWIpath}/NDWIList.txt"


    try:
        os.system(command)
        print(command)
        tile, date = readLastRowNDWI(NDWIpath)
        ti.xcom_push(key='tile_date', value=[tile,date])

    except Exception as e:
        print("Exception error while processing update list of NDWI from S3")

def createListDownload(tile, date):
    print("Create List Download ", tile, date)
    outputFolder = "/home/airflow/raw"

    if not os.path.exists(outputFolder+"/"+tile):
        os.mkdir(outputFolder+"/"+tile)
    b2 =  f"gsutil cp -r 'gs://gcp-public-data-sentinel-2/L2/tiles/{tile[0:2]}/{tile[2]}/{tile[3:]}/S2*_MSIL2A_{date}*/GRANULE/*/IMG_DATA/R10m/*B02*.jp2' {outputFolder}/{tile}/{date}/{date}_B02.jp2"
    b3 =  f"gsutil cp -r 'gs://gcp-public-data-sentinel-2/L2/tiles/{tile[0:2]}/{tile[2]}/{tile[3:]}/S2*_MSIL2A_{date}*/GRANULE/*/IMG_DATA/R10m/*B03*.jp2' {outputFolder}/{tile}/{date}/{date}_B03.jp2"
    b4 =  f"gsutil cp -r 'gs://gcp-public-data-sentinel-2/L2/tiles/{tile[0:2]}/{tile[2]}/{tile[3:]}/S2*_MSIL2A_{date}*/GRANULE/*/IMG_DATA/R10m/*B04*.jp2' {outputFolder}/{tile}/{date}/{date}_B04.jp2"
    b8 =  f"gsutil cp -r 'gs://gcp-public-data-sentinel-2/L2/tiles/{tile[0:2]}/{tile[2]}/{tile[3:]}/S2*_MSIL2A_{date}*/GRANULE/*/IMG_DATA/R10m/*B08*.jp2' {outputFolder}/{tile}/{date}/{date}_B08.jp2"
    tci = f"gsutil cp -r 'gs://gcp-public-data-sentinel-2/L2/tiles/{tile[0:2]}/{tile[2]}/{tile[3:]}/S2*_MSIL2A_{date}*/GRANULE/*/IMG_DATA/R10m/*TCI*.jp2' {outputFolder}/{tile}/{date}/{date}_tci.jp2"
    scl = f"gsutil cp -r 'gs://gcp-public-data-sentinel-2/L2/tiles/{tile[0:2]}/{tile[2]}/{tile[3:]}/S2*_MSIL2A_{date}*/GRANULE/*/IMG_DATA/R20m/*SCL*.jp2' {outputFolder}/{tile}/{date}/{date}_scl.jp2"
    meta = f"gsutil cp -r 'gs://gcp-public-data-sentinel-2/L2/tiles/{tile[0:2]}/{tile[2]}/{tile[3:]}/S2*_MSIL2A_{date}*/MTD_MSIL2A.xml' {outputFolder}/{tile}/{date}/{date}_meta.xml"

    listdownload_command = [b2, b3, b4, b8, tci, scl, meta]
    return listdownload_command

def downloadS2(ti):

    tile, backdate = ti.xcom_pull(key="tile_date", task_ids=["updateListNDWI_process"])[0]
    today = str(datetime.today().date()).replace("-","")
    list_date = pd.date_range(start=backdate, end=today)
    list_date = sorted([str(i.date()).replace("-","") for i in list_date])
    list_date.pop(0)
    print(list_date)
    print(f"start : {backdate} and enddate : {today}")
    for date_i in list_date[1:]:
        listDownload_command = createListDownload(tile, date_i)
        for i in listDownload_command:
            print("Download Command ", i)
            os.system(i)
    ti.xcom_push(key='tile_date', value=[tile,list_date])
def calculateNDWI(B8:str, B4:str):
     try:
        np.seterr(invalid='ignore')
        B8_src = rio.open(B8)
        B4_src = rio.open(B4)
        B8_arr = B8_src.read(1)
        B4_arr = B4_src.read(1)

        ndwi = (B8_arr.astype(np.float32)-B4_arr.astype(np.float32))/(B8_arr+B4_arr)
        ndwi_src = B4_src.profile
        print(B4, B8)
        print(ndwi_src)
        ndwi_src.update(count=1,compress='lzw', driver="GTiff",dtype=rio.float32)

        B8_src.close()
        B4_src.close()

        return ndwi_src, ndwi

     except:
        print("Read Image Error B8 :", B8, datetime.today())
        print("Read Image Error B4 :", B4, datetime.today())

def processndwi(ti):
    tile, backdate = ti.xcom_pull(key="tile_date", task_ids=["downloadS2_process"])[0]
    fullpath = "home/airflow"
    print(backdate)
    raw_folder = "raw"
    ndwi_folder = "ndwi"
    available_date = []
    if len(backdate)!=0:
        for i in backdate:
            #read from raw folder
            print(i)
            if os.path.exists(f"{fullpath}/{raw_folder}/{tile}/{i}"):
                available_date.append(i) #append date
                print(f"path exist: {fullpath}/{raw_folder}/{tile}/{i}")

                b8 = f"{fullpath}/{raw_folder}/{tile}/{i}/{i}_B08.jp2"
                b4 = f"{fullpath}/{raw_folder}/{tile}/{i}/{i}_B04.jp2"


                ndwi_src, ndwi = calculateNDWI(b8, b4)
                ndwi = ndwi.astype(np.float16)

                if not os.path.exists(f"{fullpath}/{ndwi_folder}"):
                    os.mkdir(f"{fullpath}/{ndwi_folder}")

                if not os.path.exists(f"{fullpath}/{ndwi_folder}/{tile}"):
                    os.mkdir(f"{fullpath}/{ndwi_folder}/{tile}")


                print("writing ndwi filename: ", f"{fullpath}/{ndwi_folder}/{tile}/{i}_NDWI.tif")

                with rio.open(f"{fullpath}/{ndwi_folder}/{tile}/{i}_NDWI.tif","w",**ndwi_src) as dst:
                    dst.write(ndwi.astype(rio.float32),1)
                    print("Wrote ndwi successfully")

        ti.xcom_push(key='available_date', value=[tile,available_date])

def removeCloud(scl, band):

    try:
        scl_src = rio.open(scl)
        band_src = rio.open(band)
        #scl_img = scl_src.read(1) #keep scl only 4,5 6
        band_img = band_src.read()
        band_img = band_img.astype(np.float16)
        meta = band_src.profile
        #upsamling SCL
        data = scl_src.read(out_shape=(scl_src.count,
                                        int(scl_src.height*2), int(scl_src.width*2)),
                                        resampling=Resampling.nearest)
        #transform = scl_src.transform*scl_src.transform.scale((scl_src.width/data.shape[-1]), (scl_src.height/data.shape[-2]))
        band_img[(data<4)|(data>=7)] = np.nan
        band_img = band_img.astype(np.float16)
        meta.update(dtype=rio.float32, nodata=np.nan,compress='lzw')
        clremoved_meta = meta.copy()
        print("Cloud was removed")
        scl_src.close()
        band_src.close()

        return band_img, clremoved_meta

    except Exception as e:
        print("error happened in removeCloud process")

def processCloudremoved(ti):
    tile, available_date = ti.xcom_pull(key="available_date",task_ids=["writeNDWI_process"])[0]
    fullpath = "home/airflow"
    ndwi_folder = "ndwi"
    raw_folder = "raw"
    ndwiRC_folder = "ndwiRC"

    NDWIrc_list = []
    if len(available_date)!=0:
        for date in available_date:
            print("available date",date)
            scl_band = f"{fullpath}/{raw_folder}/{tile}/{date}/{date}_scl.jp2"
            ndwi_band = f"{fullpath}/{ndwi_folder}/{tile}/{date}_NDWI.tif"
            ndwirc_band = f"{fullpath}/{ndwiRC_folder}/{tile}/{date}_NDWI.tif"

            if not os.path.exists(f"{fullpath}/{ndwiRC_folder}"):
                os.mkdir(f"{fullpath}/{ndwiRC_folder}")

            if not os.path.exists(f"{fullpath}/{ndwiRC_folder}/{tile}"):
                os.mkdir(f"{fullpath}/{ndwiRC_folder}/{tile}")

            band_img, meta = removeCloud(scl_band, ndwi_band)
            with  rio.open(NDWIrc_band,"w",**meta) as dst:
                dst.write(band_img.astype(rio.float32))
                print("NDWIrc was written successfully")
            ndwirc_list.append(NDWIrc_band)

        ti.xcom_push(key='NDWIrc_list', value=[tile,NDWIrc_list])
def cld_free_rgb(ti):
    tile, backdate = ti.xcom_pull(key="tile_date", task_ids=["downloadS2_process"])[0]
    fullpath = "home/airflow"
    print(backdate)
    raw_folder = "raw"
    rgb_folder = "rgbRC"
    available_date = []
    if len(backdate)!=0:
        for i in backdate:

        xds = rioxarray.open_rasterio(f"{fullpath}/{raw_folder}/{tile}/{i}/{i}_scl.jp2")
        new_width = xds.rio.width * 2
        new_height = xds.rio.height * 2
        xds_upsampled = xds.rio.reproject(
            xds.rio.crs,
            shape=(new_height, new_width),
            resampling=Resampling.bilinear,
        )
        scl_upsample=xds_upsampled
        img1=rioxarray.open_rasterio(f"{fullpath}/{raw_folder}/{tile}/{i}/{i}_TCI.jp2")[0]
        img2=rioxarray.open_rasterio(f"{fullpath}/{raw_folder}/{tile}/{i}/{i}_TCI.jp2")[1]
        img3=rioxarray.open_rasterio(f"{fullpath}/{raw_folder}/{tile}/{i}/{i}_TCI.jp2")[2]
        imgscl=scl_upsample[0]
        img1[(imgscl<4)|(imgscl>=7)] = np.nan
        img2[(imgscl<4)|(imgscl>=7)] = np.nan
        img3[(imgscl<4)|(imgscl>=7)] = np.nan
        bands = ['B', 'G', 'R']
        xr_dataset = xr.Dataset()
        xr_dataset[bands[0]] = img1
        xr_dataset[bands[1]] = img2
        xr_dataset[bands[2]] = img3
        output_band = f"{fullpath}/{rgb_folder}/{tile}/{i}_tciRC.tif"
        xr_dataset.where(xr_dataset!=0,np.nan).rio.to_raster(output_band)

def uploadS3_ndwi(ti):
    tile, available_date = ti.xcom_pull(key="available_date",task_ids=["writeNDWI_process"])[0]
    fullpath = "home/airflow"
    ndwiRC_folder = "ndwiRC"
    if len(available_date)!=0:
        try:
            for date in available_date:
                bucket = f"s3://nsi-satellite-prototype/sprectral_index/NDWI/{tile}/{date}/{date}_NDWI.tif"
                source = f"{fullpath}/{ndwiRC_folder}/{tile}/{date}_NDWI.tif"
                os.system(f"aws s3 cp {source} {bucket}")
                print("uploaded to S3: ", bucket)
        except Exception as e:
            print("Error happened during upload to S3")

def uploadS3_tci(ti):
    tile, available_date = ti.xcom_pull(key="available_date",task_ids=["writeNDWI_process"])[0]
    fullpath = "home/airflow"
    rgbRC_folder = "rgbRC"
    if len(available_date)!=0:
        try:
            for date in available_date:
                bucket = f"s3://nsi-satellite-prototype/sprectral_index/NDWI/{tile}/{date}/{date}_tciRC.tif"
                source = f"{fullpath}/{ndwiRC_folder}/{tile}/{date}_tciRC.tif"
                os.system(f"aws s3 cp {source} {bucket}")
                print("uploaded to S3: ", bucket)
        except Exception as e:
            print("Error happened during upload to S3")

with DAG("NSI_preprocess", start_date=datetime(2022,7,11),schedule_interval="@daily", catchup=False) as dag:
    updateNDWI = PythonOperator(task_id="updateListNDWI_process", python_callable=updateListNDWI)
    download_s2 = PythonOperator(task_id="downloadS2_process",python_callable=downloadS2)
    writeNDWIFile = PythonOperator(task_id="writeNDWI_process", python_callable=processNDWI)
    writeNDWIRCFile = PythonOperator(task_id="writeNDWIRC_process", python_callable=processCloudremoved)
    uploadtoS3_ndwi = PythonOperator(task_id="uploadS3_ndwi_process", python_callable=uploadS3)
    rgb_RC=PythonOperator(task_id="tci_process", python_callable=cld_free_rgb)
    uploadtoS3_tci = PythonOperator(task_id="uploadS3_tci_process", python_callable=uploadS3_tci)
 updateNDWI >> download_s2 >> writeNDWIFile >>writeNDVIRCFile >> uploadtoS3_ndwi >> rgb_RC >> uploadtoS3_tci