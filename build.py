import argparse
from datetime import timedelta
import multiprocessing
from time import strftime
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
RESIZED_IMAGE_BASE_PATH = Path("ambientcg")
IN_ZIP_IMAGE_PATH = "ambientcg"
SH3T_PACKAGE_BASE_PATH = Path("ambientcg.sh3t")
CATALOG_FILE_PATH = Path("PluginTexturesCatalog.properties")
PYPROJECT_PATH = Path("pyproject.toml")

ORIGINAL_IMAGES_PATH.mkdir(exist_ok=True)

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


def get_asset_zip_url(asset, quality=1):
    """
    From JSON "asset" data return zip file Url and image to retrieve
    Search for lesser quality possible and for xxx_Color.png or xxx_var1.png file
    """
    downloads = asset["downloadFolders"]["default"]["downloadFiletypeCategories"][
        "zip"
    ]["downloads"]

    png_attribute = f"{quality}K-PNG"
    jpg_attribute = f"{quality}K-JPG"

    for download in downloads:
        if download["attribute"] == png_attribute:
            png_filename = check_file(
                download.get("zipContent", []),
                ["Color.png", "Normal.png", "var1.png"]
            )
            if png_filename:
                return download["fullDownloadPath"], png_filename

        if download["attribute"] == jpg_attribute:
            jpg_filename = check_file(
                download.get("zipContent", []),
                ["Color.jpg", "Normal.png", "Normal.jpg", "var1.jpg"]
            )
            if jpg_filename:
                return download["fullDownloadPath"], jpg_filename

    if quality < 16:
        return get_asset_zip_url(asset, quality + 1)

    print ("------------------------------------------------------------")
    print('asset: %s', json.dumps(asset))
    print(f"Quality {quality}");
    exit()

    return None, None


def get_asset_data(asset):
    """
    From JSON asset return dict containing SH3D texture catalog data
    """
    zip_url, color_filename = get_asset_zip_url(asset)

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
                catalog_data.append(get_asset_data(asset))
            except Exception as e:
                print(f"ERROR: {e}")

        last_fetch_count = len(json_data["foundAssets"])
        offset += JSON_BATCH_SIZE
        if TOTAL_LIMIT and offset >= TOTAL_LIMIT:
            break

    return sorted(catalog_data, key=lambda entry: entry["assetId"])


def download_images(catalog_data, options):
    """
    Download all zip and extract images from given metadata
    """

    for entry in catalog_data:
        dest_path = ORIGINAL_IMAGES_PATH / entry["image_filename"]
        if not options.no_image_cache and dest_path.exists():
            continue
        print(entry["zip_url"])

        category_path = ORIGINAL_IMAGES_PATH / entry["category"]
        category_path.mkdir(exist_ok=True)

        file_path = category_path / entry["zip_url"].split("/")[-1]

        if file_path.exists():
            print(f"EXIST: {file_path}")
            continue

        print(f"LOAD: {file_path}")

        zip_file_response = requests.get(entry["zip_url"])

        with open(file_path, "wb") as f:
            f.write(zip_file_response.content)

def build_texture_lib(options):
    catalog_data = fetch_catalog_data(options)
    version = get_version(options)
    download_images(catalog_data, options)
    write_version(version)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--no-json-cache", action="store_true")
    parser.add_argument("--no-image-cache", action="store_true")
    parser.add_argument("--no-version", action="store_true")
    options = parser.parse_args()
    build_texture_lib(options)
