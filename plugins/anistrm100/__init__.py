import os
import time
from datetime import datetime, timedelta
import re # <<< NEW: Import re for parsing season from URL

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.utils.http import RequestUtils
from app.core.config import settings
from app.plugins import _PluginBase
from typing import Any, List, Dict, Tuple, Optional
from app.log import logger
import xml.dom.minidom
from app.utils.dom import DomUtils

def retry(ExceptionToCheck: Any,
          tries: int = 3, delay: int = 3, backoff: int = 1, logger: Any = None, ret: Any = None):
    def deco_retry(f):
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 0:
                try:
                    return f(*args, **kwargs)
                except ExceptionToCheck as e:
                    msg = f"An error occurred, retrying in {mdelay} seconds... Error: {e}"
                    if logger:
                        logger.warn(msg)
                    else:
                        print(msg)
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            if logger:
                logger.warn('Failed after multiple retries. Please check folder existence or network issues.')
            return ret
        return f_retry
    return deco_retry

class ANiStrm100(_PluginBase):
    plugin_name = "ANiStrm100"
    plugin_desc = "Automatically fetches all anime of the season, creating a media library without downloading."
    plugin_icon = "https://raw.githubusercontent.com/honue/MoviePilot-Plugins/main/icons/anistrm.png"
    plugin_version = "2.6.0" # <<< MODIFIED: Version bump
    plugin_author = "GlowsSama & Gemini"
    author_url = "https://github.com/honue"
    plugin_config_prefix = "anistrm100_"
    plugin_order = 15
    auth_level = 2

    _enabled = False
    _cron = None
    _onlyonce = False
    _fulladd = False
    _allseason = False
    _storageplace = None

    _scheduler: Optional[BackgroundScheduler] = None

    def init_plugin(self, config: dict = None):
        self.stop_service()
        if config:
            self._enabled = config.get("enabled")
            self._cron = config.get("cron")
            self._onlyonce = config.get("onlyonce")
            self._fulladd = config.get("fulladd")
            self._allseason = config.get("allseason")
            self._storageplace = config.get("storageplace")
        if self._enabled or self._onlyonce:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            if self._enabled and self._cron:
                try:
                    self._scheduler.add_job(func=self.__task,
                                            trigger=CronTrigger.from_crontab(self._cron),
                                            name="ANiStrm100 File Creation")
                    logger.info(f'ANi-Strm scheduled task created successfully: {self._cron}')
                except Exception as err:
                    logger.error(f"Cron configuration error: {str(err)}")
            if self._onlyonce:
                logger.info(f"ANi-Strm service started, running once immediately.")
                self._scheduler.add_job(func=self.__task,
                                        args=[self._fulladd, self._allseason],
                                        trigger='date',
                                        run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
                                        name="ANiStrm100 File Creation")
                self._onlyonce = False
                self._fulladd = False
                self._allseason = False
            self.__update_config()
            if self._scheduler.get_jobs():
                self._scheduler.print_jobs()
                self._scheduler.start()

    def __get_ani_season(self, idx_month: int = None) -> str:
        current_date = datetime.now()
        current_year = current_date.year
        current_month = idx_month if idx_month else current_date.month
        # This logic determines the *start* month of the current season
        if 1 <= current_month <= 3:
            season_month = 1
        elif 4 <= current_month <= 6:
            season_month = 4
        elif 7 <= current_month <= 9:
            season_month = 7
        else: # 10, 11, 12
            season_month = 10
        self._date = f'{current_year}-{season_month}'
        return self._date

    def __is_valid_file(self, name: str) -> bool:
        # This is your rule: a downloadable file must contain "ANi"
        return 'ANi' in name

    # <<< NEW: Core recursive traversal function >>>
    @retry(Exception, tries=3, logger=logger, ret=[])
    def __traverse_directory(self, path_parts: List[str]) -> List[Tuple[str, List[str], str]]:
        """
        Recursively traverses directories to find all valid files.
        :param path_parts: A list of path components, e.g., ['2024-4'] or ['2024-4', 'ShowName'].
        :return: A list of tuples, each containing (season, sub_path_list, file_name).
        """
        all_files = []
        current_path_str = "/".join(path_parts)
        url = f'https://openani.an-i.workers.dev/{current_path_str}/'
        
        logger.debug(f"Traversing: {url}")
        rep = RequestUtils(ua=settings.USER_AGENT, proxies=settings.PROXY).post(url=url)
        # Assuming the API returns a JSON object with a 'files' key, even if it fails gracefully
        items = rep.json().get('files', [])

        season_str = path_parts[0] # The first part is always the season, e.g., '2024-4'
        sub_path_list = path_parts[1:] # The rest are subdirectories

        for item in items:
            item_name = item.get('name')
            if not item_name:
                continue

            if self.__is_valid_file(item_name):
                # This is a file, add it to our results
                all_files.append((season_str, sub_path_list, item_name))
            else:
                # This is likely a directory, traverse into it
                # The 'size' for folders is often 0 or not present, but checking the name is your defined rule
                # To avoid infinite loops or recursion on weird folder names, we add a simple check.
                if '.' not in item_name: # Simple heuristic to guess if it's a folder
                     all_files.extend(self.__traverse_directory(path_parts + [item_name]))
        
        return all_files

    # <<< MODIFIED: This now uses the recursive traversal function >>>
    def get_current_season_list(self) -> List[Tuple[str, List[str], str]]:
        season = self.__get_ani_season()
        logger.info(f"Getting file list for current season: {season}")
        return self.__traverse_directory([season])

    @retry(Exception, tries=3, logger=logger, ret=[])
    def get_latest_list(self) -> List:
        addr = 'https://api.ani.rip/ani-download.xml'
        ret = RequestUtils(ua=settings.USER_AGENT, proxies=settings.PROXY).get_res(addr)
        dom_tree = xml.dom.minidom.parseString(ret.text)
        items = dom_tree.documentElement.getElementsByTagName("item")
        result = []
        for item in items:
            title = DomUtils.tag_value(item, "title", default="")
            link = DomUtils.tag_value(item, "link", default="")
            # Try to extract season and path from the link itself for better sorting
            # Example link: https://.../2024-4/ShowName/File.mp4
            season_match = re.search(r'/(\d{4}-\d{1,2})/', link)
            if season_match:
                full_path = link.split(season_match.group(0))[-1]
                path_parts = full_path.split('/')
                file_name = path_parts.pop() # remove last element (filename)
                # Let's ensure the title from the RSS matches the filename from the link
                if title in file_name:
                     result.append({
                        'season': season_match.group(1),
                        'path_parts': path_parts,
                        'title': title, # Keep original title
                        'link': link.replace("resources.ani.rip", "openani.an-i.workers.dev")
                    })
        return result

    # <<< MODIFIED: This now uses the recursive traversal function >>>
    def get_all_season_list(self, start_year: int = 2018) -> List[Tuple[str, List[str], str]]:
        now = datetime.now()
        all_files = []
        for year in range(start_year, now.year + 1):
            for month in [1, 4, 7, 10]:
                if year == now.year and month > now.month:
                    continue # Don't check future seasons
                season = f"{year}-{month}"
                logger.info(f"Getting file list for season: {season}")
                try:
                    season_files = self.__traverse_directory([season])
                    if season_files:
                        all_files.extend(season_files)
                except Exception as e:
                    logger.warn(f"Failed to get anime for season {season}: {e}")
        return all_files

    # <<< MODIFIED: Updated to handle a list of subdirectories >>>
    def __touch_strm_file(self, file_name: str, season: str, sub_paths: List[str] = None, file_url: str = None) -> bool:
        sub_paths = sub_paths or []
        
        # Build the local directory path, including subdirectories
        # os.path.join correctly handles the list of sub_paths with the * operator
        dir_path = os.path.join(self._storageplace, season, *sub_paths)
        os.makedirs(dir_path, exist_ok=True)

        # Construct the source URL for the .strm file content
        if file_url:
            src_url = file_url
        else:
            remote_path = "/".join([season] + sub_paths + [file_name])
            src_url = f'https://openani.an-i.workers.dev/{remote_path}?d=true'

        file_path = os.path.join(dir_path, f'{file_name}.strm')
        if os.path.exists(file_path):
            logger.debug(f'{file_name}.strm already exists')
            return False
        
        try:
            with open(file_path, 'w', encoding='utf-8') as file:
                file.write(src_url)
            logger.debug(f'Successfully created .strm file: {file_path}')
            return True
        except Exception as e:
            logger.error(f'Failed to create .strm file {file_path}: {e}')
            return False

    # <<< MODIFIED: Main task logic updated for the new data structures >>>
    def __task(self, fulladd: bool = False, allseason: bool = False):
        cnt = 0
        file_list = []

        if allseason:
            logger.info("Starting task: Create strm for ALL historical seasons.")
            file_list = self.get_all_season_list()
            logger.info(f'Processing all seasons, found {len(file_list)} total files.')
            for season, path_parts, file_name in file_list:
                if self.__is_valid_file(file_name):
                    if self.__touch_strm_file(file_name=file_name, season=season, sub_paths=path_parts):
                        cnt += 1
        elif fulladd:
            logger.info("Starting task: Create strm for ALL files in the CURRENT season.")
            file_list = self.get_current_season_list()
            logger.info(f'Processing current season, found {len(file_list)} files.')
            for season, path_parts, file_name in file_list:
                if self.__is_valid_file(file_name):
                    if self.__touch_strm_file(file_name=file_name, season=season, sub_paths=path_parts):
                        cnt += 1
        else:
            logger.info("Starting task: Fetch latest files from RSS feed.")
            rss_info_list = self.get_latest_list()
            logger.info(f'Processing RSS feed, {len(rss_info_list)} new items found.')
            for rss_info in rss_info_list:
                if self.__is_valid_file(rss_info['title']):
                    if self.__touch_strm_file(file_name=rss_info['title'], 
                                              file_url=rss_info['link'], 
                                              season=rss_info['season'], 
                                              sub_paths=rss_info['path_parts']):
                        cnt += 1
        
        logger.info(f'Task finished. Created {cnt} new .strm files.')

    def get_state(self) -> bool:
        return self._enabled

    def get_command(self) -> List[Dict[str, Any]]:
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        pass

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        # This part remains unchanged
        return [
            {
                'component': 'VForm',
                'content': [
                    {
                        'component': 'VRow',
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 3}, 'content': [{'component': 'VSwitch', 'props': {'model': 'enabled', 'label': 'Enable Plugin'}}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 3}, 'content': [{'component': 'VSwitch', 'props': {'model': 'onlyonce', 'label': 'Run Once Now'}}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 3}, 'content': [{'component': 'VSwitch', 'props': {'model': 'fulladd', 'label': 'Create .strm for all in current season'}}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 3}, 'content': [{'component': 'VSwitch', 'props': {'model': 'allseason', 'label': 'Create .strm for all historical seasons'}}]}
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [{'component': 'VTextField', 'props': {'model': 'cron', 'label': 'Execution Schedule (Cron)', 'placeholder': '0 0 ? ? ?'}}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [{'component': 'VTextField', 'props': {'model': 'storageplace', 'label': 'Strm Storage Path', 'placeholder': '/downloads/strm'}}]}
                        ]
                    }
                ]
            }
        ], {
            "enabled": False,
            "onlyonce": False,
            "fulladd": False,
            "allseason": False,
            "storageplace": "/downloads/strm",
            "cron": "*/20 22,23,0,1 * * *",
        }

    def __update_config(self):
        self.update_config({
            "onlyonce": self._onlyonce,
            "cron": self._cron,
            "enabled": self._enabled,
            "fulladd": self._fulladd,
            "allseason": self._allseason,
            "storageplace": self._storageplace,
        })

    def get_page(self) -> List[dict]:
        pass

    def stop_service(self):
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._scheduler.shutdown()
                self._scheduler = None
        except Exception as e:
            logger.error("Failed to stop plugin: %s" % str(e))

if __name__ == "__main__":
    # Example of how to test the new logic
    class MockLogger:
        def info(self, msg): print(f"INFO: {msg}")
        def warn(self, msg): print(f"WARN: {msg}")
        def error(self, msg): print(f"ERROR: {msg}")
        def debug(self, msg): print(f"DEBUG: {msg}")

    logger = MockLogger()
    
    anistrm100 = ANiStrm100()
    # Configure it for testing
    anistrm100._storageplace = "./strm_test"
    anistrm100.settings = lambda: None # Mock settings
    anistrm100.settings.USER_AGENT = "Mozilla/5.0"
    anistrm100.settings.PROXY = None
    
    print("--- Testing get_all_season_list (starting from a recent year for speed) ---")
    all_files = anistrm100.get_all_season_list(start_year=2024)
    for season, path_parts, file_name in all_files[:5]: # Print first 5 results
        print(f"Season: {season}, Path: {'/'.join(path_parts)}, File: {file_name}")
    
    print("\n--- Simulating task run (fulladd) ---")
    anistrm100.__task(fulladd=True)
