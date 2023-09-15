import argparse
from datetime import timedelta
import multiprocessing
from time import strftime
import time
import tomlkit
import zipfile
from functools import partial
from io import BytesIO
from itertools import product
from pathlib import Path

import json
import requests
import requests_cache
from jinja2 import Environment, FileSystemLoader, select_autoescape
from PIL import Image, ImageOps

from excluded_categories import EXCLUDED_CATEGORIES
from make_preview import make_preview

import re
import os
import pdb

SIZES = (1024, 512, 256)
DOWNLOAD_URLS = {
    1024: "https://fenouil-drive.mycozy.cloud/public?sharecode=QyiDq6EIACQi",
    512: "https://fenouil-drive.mycozy.cloud/public?sharecode=u2Ps0zAB8cij",
    256: "https://fenouil-drive.mycozy.cloud/public?sharecode=3gsbVKG3PFBM",
}
TOTAL_LIMIT = None

JSON_BATCH_SIZE = 100
BASE_URL = "https://ambientcg.com/api/v2/full_json"
ORIGINAL_IMAGES_PATH = Path("ambientcg_originals")
TEXTURES_PATH = Path("ambientcg_textures")
RESIZED_IMAGE_BASE_PATH = Path("ambientcg")
IN_ZIP_IMAGE_PATH = "ambientcg"
SH3T_PACKAGE_BASE_PATH = Path("ambientcg.sh3t")
CATALOG_FILE_PATH = Path("PluginTexturesCatalog.properties")
PYPROJECT_PATH = Path("pyproject.toml")

ORIGINAL_IMAGES_PATH.mkdir(exist_ok=True)

TEXTURES_PATH.mkdir(exist_ok=True)

def get_version(options):
    """
    return today version
    """
    if options.no_version:
        pyproject_toml = tomlkit.parse(PYPROJECT_PATH.read_text(encoding="UTF-8"))
        return pyproject_toml["tool"]["poetry"]["version"]

    return strftime("%Y.%m.%d")

def write_version(version):
    print(f"Version: {version}")
    pyproject_toml = tomlkit.parse(PYPROJECT_PATH.read_text(encoding="UTF-8"))
    pyproject_toml["tool"]["poetry"]["version"] = version
    PYPROJECT_PATH.write_text(tomlkit.dumps(pyproject_toml), encoding="UTF-8")


def check_file(zip_content, endswith_strings):
    """
    Return JSON zipContent item end with one of endsWith string
    """
    for filename, endswith_string in product(zip_content, endswith_strings):
        if filename.endswith(endswith_string):
            return filename
    return None


def get_asset_zip_url(asset, image_type, ignore_missing, quality):
    """
    From JSON "asset" data return zip file Url and image to retrieve
    Search for lesser quality possible and for xxx_Color.png or xxx_var1.png file
    """
    downloads = asset["downloadFolders"]["default"]["downloadFiletypeCategories"][
        "zip"
    ]["downloads"]

    type_attr_ext = image_type.upper()
    type_attribute = f"{quality}K-{type_attr_ext}"
    type_ext = image_type.lower()

    for download in downloads:
        if download["attribute"] == type_attribute:
            image_names = [f"Color.{type_ext}", f"Normal.{type_ext}", "Normal.png", f"var1.{type_ext}"]
            #print("check: ", json.dumps(image_names))
            image_filename = check_file(
                download.get("zipContent", []),
                image_names
            )
            if image_filename:
                #print('FOUND:', json.dumps(asset))
                print('FOUND:', image_filename)
                #print('FOUND:', download["downloadLink"])
                #exit()
                return download["downloadLink"], image_filename

    if quality < 16:
        return get_asset_zip_url(asset, image_type, ignore_missing, quality + 1)

    if not ignore_missing:
        print("------------------------------------------------------------")
        print('MISSING:', json.dumps(asset))
        exit()
        #raise Exception('MISSING')

    return None, None


def get_asset_data(asset, image_type, ignore_missing, quality):
    """
    From JSON asset return dict containing SH3D texture catalog data
    """
    zip_url, color_filename = get_asset_zip_url(asset, image_type, ignore_missing, quality)

    if not zip_url:
        raise Exception("No zip url found")

    if asset["category"] in EXCLUDED_CATEGORIES:
        raise Exception("Excluded category")

    return {
        "catalog_infos": {
            "id": f"ambientcg#{asset['assetId']}",
            "name": asset["displayName"],
            "category": f"[ACG]{asset['category']}",
            "image": f"/{IN_ZIP_IMAGE_PATH}/{asset['assetId']}.jpg",
            "width": asset["dimensionX"] or 100,
            "height": asset["dimensionY"] or 100,
            "creator": "ambientCG.com",
        },
        "assetId": asset["assetId"],
        "category": asset["category"],
        "zip_url": zip_url,
        "in_zip_color_filename": color_filename,
        "image_filename": f"{asset['assetId']}.jpg",
    }


def fetch_catalog_data(options):
    """
    Fetch remote JSON of all material assets and return list of data required to
    download images and to build calalog
    """
    # expire_after = 0 if options.no_json_cache else -1
    session = requests_cache.CachedSession("requests_cache", expire_after=timedelta(days=1))
    if options.no_json_cache:
        session.cache.clear()
    session.remove_expired_responses()
    base_params = {
        "type": "Material",
        "include": "displayData,dimensionsData,downloadData",
    }
    offset = 0
    last_fetch_count = 1
    catalog_data = []
    while last_fetch_count:
        params = {
            **base_params,
            "limit": JSON_BATCH_SIZE,
            "offset": offset,
        }
        request = requests.Request("GET", BASE_URL, params=params)
        prepared_request = session.prepare_request(request)
        print(prepared_request.url)
        response = session.send(prepared_request)
        json_data = response.json()

        for asset in json_data["foundAssets"]:
            print(asset["assetId"])
            try:
                # with ipdb.launch_ipdb_on_exception():
                catalog_data.append(get_asset_data(
                    asset,
                    options.image_type,
                    options.ignore_missing,
                    options.quality))
            except Exception as e:
                print(f"ERROR: {e}")

        last_fetch_count = len(json_data["foundAssets"])
        offset += JSON_BATCH_SIZE
        if TOTAL_LIMIT and offset >= TOTAL_LIMIT:
            break

    return sorted(catalog_data, key=lambda entry: entry["assetId"])


def download_image(entry, options):
    zip_url = entry["zip_url"]
    dest_path = ORIGINAL_IMAGES_PATH / entry["image_filename"]
    #if not options.no_image_cache and dest_path.exists():
    #    continue

    category_path = ORIGINAL_IMAGES_PATH / entry["category"]
    category_path.mkdir(exist_ok=True)

    file_path = category_path / zip_url.split("=")[-1]

    if not options.no_image_cache and file_path.exists():
        print(f"EXIST: {file_path}")
        return False

    print(f"LOAD:  {file_path} - {zip_url}")

    zip_file_response = requests.get(zip_url)
    zip_size = len(zip_file_response.content)

    print(f"SIZE:  {zip_size}")

    file_header = zip_file_response.content[0:2].decode('utf-8')
    #print(f"HEADER: {file_header}")
    valid = file_header == 'PK'

    if not valid:
        error_text = str(zip_file_response.content[0:20])
        raise Exception(error_text)

    with open(file_path, "wb") as f:
        f.write(zip_file_response.content)

    return True

def download_images(catalog_data, options):
    """
    Download all zip and extract images from given metadata
    """

    for entry in catalog_data:
        retry = 0
        while retry < 2:
            try:
                result = download_image(entry, options)
                retry = 2
            except Exception as e:
               print(f"ERROR: {e}")
               time.sleep(5)
               retry += 1

def unzip_image(category_path, src_path, zip_file_path, options):
    print("SRC:", src_path)
    print("ZIP:", zip_file_path)

    category_dst_path = TEXTURES_PATH / category_path
    category_dst_path.mkdir(exist_ok=True)

    texture_path = re.sub("\.zip", "", zip_file_path)
    dst_path = category_dst_path / texture_path
    print("DST:", dst_path)

    dst_path.mkdir(exist_ok=True)

    with zipfile.ZipFile(src_path) as zip:
        zip.extractall(dst_path)

def unzip_images(catalog_data, options):
    category_paths = os.listdir(ORIGINAL_IMAGES_PATH)

    for category_path in sorted(category_paths):
        category_src_path = ORIGINAL_IMAGES_PATH / category_path
        zip_file_paths = os.listdir(category_src_path)

        for zip_file_path in sorted(zip_file_paths):
            src_path = category_src_path / zip_file_path
            unzip_image(category_path, src_path, zip_file_path, options)

def build_texture_lib(options):
    catalog_data = fetch_catalog_data(options)
    version = get_version(options)
    if options.download:
        download_images(catalog_data, options)
    if options.unzip:
        unzip_images(catalog_data, options)
    write_version(version)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--quality", type=int, default=1, action="store")
    parser.add_argument("--image-type", required=True, action="store")
    parser.add_argument("--ignore-missing", action="store_true")
    parser.add_argument("--no-json-cache", action="store_true")
    parser.add_argument("--no-image-cache", action="store_true")
    parser.add_argument("--no-version", action="store_true")
    parser.add_argument("--download", action="store_true")
    parser.add_argument("--unzip", action="store_true")
    options = parser.parse_args()
    build_texture_lib(options)
