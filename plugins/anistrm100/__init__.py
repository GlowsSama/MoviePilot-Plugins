import os
import time
from datetime import datetime, timedelta
import re 
import shutil # 导入 shutil 模块
import tempfile # 导入 tempfile 模块
import urllib.parse # 导入 URL 解码库

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

# 重试装饰器
def retry(ExceptionToCheck: Any,
          tries: int = 3, delay: int = 5, backoff: int = 1, logger: Any = None, ret: Any = None): # 延迟从 3 增加到 5
    def deco_retry(f):
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 0:
                try:
                    return f(*args, **kwargs)
                except ExceptionToCheck as e:
                    msg = f"发生错误，将在 {tries - mtries + 1}/{tries} 次重试中，等待 {mdelay} 秒... 错误详情: {e}" # 优化日志
                    if logger:
                        logger.warn(msg)
                    else:
                        print(msg)
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            if logger:
                logger.error('多次重试后仍然失败。请检查网络连接、代理设置或目标服务是否可用。') # 更改为 error 级别
            return ret
        return f_retry
    return deco_retry

class ANiStrm100(_PluginBase):
    plugin_name = "ANiStrm100"
    plugin_desc = "自动获取当季所有番剧，免去下载，轻松拥有一个番剧媒体库"
    plugin_icon = "https://raw.githubusercontent.com/honue/MoviePilot-Plugins/main/icons/anistrm.png"
    plugin_version = "2.8.4" # 版本更新
    plugin_author = "honue,GlowsSama"
    author_url = "https://github.com/GlowsSama"
    plugin_config_prefix = "anistrm100_"
    plugin_order = 15
    auth_level = 2

    _enabled = False
    _cron = None
    _onlyonce = False
    _fulladd = False
    _allseason = False
    _storageplace = None
    _overwrite = False 

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
            self._overwrite = config.get("overwrite", False) 
        
        if self._enabled or self._onlyonce:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            if self._enabled and self._cron:
                try:
                    self._scheduler.add_job(func=self.__task,
                                             args=[self._fulladd, self._allseason], # Cron 任务使用持久化配置
                                             trigger=CronTrigger.from_crontab(self._cron),
                                             name="ANiStrm100文件创建")
                    logger.info(f'ANi-Strm定时任务创建成功：{self._cron}')
                except Exception as err:
                    logger.error(f"定时任务配置错误：{str(err)}")
            
            if self._onlyonce:
                logger.info(f"ANi-Strm服务启动，立即运行一次")
                # 立即运行一次的任务，其 fulladd 和 allseason 参数取决于 UI 勾选状态
                self._scheduler.add_job(func=self.__task,
                                         args=[self._fulladd, self._allseason], # 传递当前 UI 勾选状态
                                         trigger='date',
                                         run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
                                         name="ANiStrm100文件创建")
                # 立即运行一次后，将 onlyonce 标志重置为 False，避免重复触发
                # fulladd 和 allseason 也重置为 False，因为它们是 onlyonce 的临时状态
                self._onlyonce = False
                self._fulladd = False 
                self._allseason = False 
            
            self.__update_config() # 保存最新的配置状态，包括重置后的 _onlyonce, _fulladd, _allseason
            if self._scheduler.get_jobs():
                self._scheduler.print_jobs()
                self._scheduler.start()

    def __get_ani_season(self, idx_month: int = None) -> str:
        current_date = datetime.now()
        current_year = current_date.year
        current_month = idx_month if idx_month else current_date.month
        
        if 1 <= current_month <= 3: season_month = 1
        elif 4 <= current_month <= 6: season_month = 4
        elif 7 <= current_month <= 9: season_month = 7
        else: season_month = 10
        self._date = f'{current_year}-{season_month}'
        return self._date

    def __is_valid_file(self, name: str) -> bool:
        return 'ANi' in name

    @retry(Exception, tries=3, delay=5, logger=logger, ret=[]) # 延迟增加
    def __traverse_directory(self, path_parts: List[str]) -> List[Tuple[str, List[str], str]]:
        all_files = []
        current_path_str = "/".join(path_parts)
        url = f'https://ani.v300.eu.org/{current_path_str}/' # URL 保持不变
        
        logger.debug(f"正在遍历: {url}")
        rep = RequestUtils(ua=settings.USER_AGENT, proxies=settings.PROXY).post(url=url)
        if rep and hasattr(rep, 'json'):
            items = rep.json().get('files', [])
        else:
            logger.warn(f"无法获取有效的响应或响应无json方法，URL: {url}。这可能是网络问题或目标服务暂时不可用。") # 优化日志
            items = [] 

        base_folder = path_parts[0]
        sub_path_list = path_parts[1:]

        for item in items:
            item_name = item.get('name')
            if not item_name: continue

            if self.__is_valid_file(item_name):
                all_files.append((base_folder, sub_path_list, item_name))
            elif '.' not in item_name:
                 all_files.extend(self.__traverse_directory(path_parts + [item_name]))
        
        return all_files

    @retry(Exception, tries=3, delay=5, logger=logger, ret=[]) # 延迟增加
    def get_latest_list(self) -> List:
        addr = 'https://aniapi.v300.eu.org/ani-download.xml'
        logger.info(f"正在尝试从 RSS 源获取最新文件: {addr}") 
        ret = RequestUtils(ua=settings.USER_AGENT, proxies=settings.PROXY).get_res(addr)
        if ret and hasattr(ret, 'text'):
            dom_tree = xml.dom.minidom.parseString(ret.text)
            items = dom_tree.documentElement.getElementsByTagName("item")
            result = []
            for item in items:
                title = DomUtils.tag_value(item, "title", default="")
                link = DomUtils.tag_value(item, "link", default="")
                
                # 确保 link 是有效的 URL
                if not link.startswith(('http://', 'https://')):
                    logger.warn(f"RSS 项目链接无效，跳过: {link}")
                    continue

                season_match = re.search(r'/(\d{4}-\d{1,2})/', link)
                if season_match:
                    # 提取文件名部分，并进行 URL 解码
                    # link 示例: https://ani.v300.eu.org/2025-4/%5BANi%5D%20Summer%20Pockets%20-%2013%20%5B1080P%5D%5BBaha%5D%5BWEB-DL%5D%5BAAC%20AVC%5D%5BCHT%5D.mp4?d=true
                    # 找到最后一个 '/' 后的文件名，并去除可能的 '?d=true'
                    parsed_url = urllib.parse.urlparse(link)
                    file_name_from_link_encoded = os.path.basename(parsed_url.path) # 获取 URL 路径的最后部分
                    file_name_from_link_decoded = urllib.parse.unquote(file_name_from_link_encoded) # URL 解码

                    # 检查 title 是否包含在解码后的文件名中
                    if title in file_name_from_link_decoded: # 使用解码后的文件名进行比较
                         result.append({
                            'season': season_match.group(1), # 例如 '2025-4'
                            'path_parts': [], # RSS 文件不再需要 sub_paths 来构建本地目录，但保留字段
                            'title': title, # 这是原始文件名，例如 '[ANi] Summer Pockets - 13 [1080P]....mp4'
                            'link': link # 直接使用原始 link 作为 .strm 内容
                        })
                    else:
                        logger.debug(f"RSS 项目名称不匹配，跳过。Title: '{title}', Link Filename: '{file_name_from_link_decoded}'")
                else:
                    logger.debug(f"RSS 项目链接未找到季度信息，跳过: {link}")
            logger.info(f"成功从 RSS 源获取到 {len(result)} 个项目。") 
            return result
        else:
            logger.warn(f"无法获取有效的RSS响应或响应无text属性，URL: {addr}。这可能是网络问题或RSS源暂时不可用。") 
            return [] 

    def get_all_season_list(self, start_year: int = 2024) -> List[Tuple[str, List[str], str]]:
        now = datetime.now()
        all_files = []
        for year in range(start_year, now.year + 1):
            for month in [1, 4, 7, 10]:
                if year == now.year and month > now.month:
                    continue
                season = f"{year}-{month}"
                logger.info(f"正在获取季度 {season} 的文件列表")
                try:
                    season_files = self.__traverse_directory([season])
                    if season_files:
                        all_files.extend(season_files)
                except Exception as e:
                    logger.warn(f"获取季度 {season} 的番剧失败: {e}")

        logger.info("正在获取 'ANi' 根目录的文件列表")
        try:
            ani_files = self.__traverse_directory(['ANi'])
            if ani_files:
                all_files.extend(ani_files)
        except Exception as e:
            logger.warn(f"获取 'ANi' 目录的文件失败: {e}")
            
        return all_files
    
    def __touch_strm_file(self, file_name: str, season: str, sub_paths: List[str] = None, file_url: str = None, overwrite: bool = False) -> bool:
        # 目标目录统一为：{Strm儲存路徑}/{季度}
        target_dir_path = os.path.join(self._storageplace, season)
        os.makedirs(target_dir_path, exist_ok=True)

        # 目标文件名统一为：原始檔名.strm
        target_file_name = f'{file_name}.strm'
        target_file_path = os.path.join(target_dir_path, target_file_name)
        
        if not overwrite and os.path.exists(target_file_path):
            logger.debug(f'{target_file_name} 文件已存在于最终目录，跳过创建。')
            return False

        if file_url:
            src_url = file_url # 对于 RSS 文件，直接使用 link 作为内容
        else:
            # 对于目录遍历的文件，构造远程 URL
            # 远程 URL 仍然需要 sub_paths 来正确指向文件
            remote_path = "/".join([season] + (sub_paths or []) + [file_name])
            src_url = f'https://ani.v300.eu.org/{remote_path}?d=true'

        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_file_path = os.path.join(temp_dir, target_file_name)
                with open(temp_file_path, 'w', encoding='utf-8') as file:
                    file.write(src_url)
                logger.debug(f'成功在临时目录创建 .strm 文件: {temp_file_path}')
                
                shutil.move(temp_file_path, target_file_path)
                logger.info(f'成功将文件从临时目录移动到: {target_file_path}')

            return True
        except Exception as e:
            logger.error(f'创建或移动 .strm 文件 {target_file_name} 失败: {e}')
            return False

    def __task(self, fulladd: bool = False, allseason: bool = False):
        cnt = 0
        
        overwrite_mode = self._overwrite # 使用插件配置的覆盖模式

        # 根据传入的 fulladd 和 allseason 参数决定执行模式
        if allseason:
            logger.info("开始任务：为所有历史季度和'ANi'目录创建strm文件。")
            file_list = self.get_all_season_list()
            logger.info(f"处理所有历史内容，共找到 {len(file_list)} 个文件。")
            for season, path_parts, file_name in file_list:
                if self.__is_valid_file(file_name):
                    if self.__touch_strm_file(file_name=file_name, season=season, sub_paths=path_parts, overwrite=overwrite_mode):
                        cnt += 1
        elif fulladd:
            logger.info("开始任务：为当前季度的所有文件创建strm文件。")
            file_list = self.get_current_season_list()
            logger.info(f'处理当前季度，共找到 {len(file_list)} 个文件。')
            for season, path_parts, file_name in file_list:
                if self.__is_valid_file(file_name):
                    if self.__touch_strm_file(file_name=file_name, season=season, sub_paths=path_parts, overwrite=overwrite_mode):
                        cnt += 1
        else: # 默认执行 RSS 获取
            logger.info("开始任务：从RSS源获取最新文件。")
            rss_info_list = self.get_latest_list()
            logger.info(f'处理RSS源，找到 {len(rss_info_list)} 个新项目。')
            for rss_info in rss_info_list:
                if self.__is_valid_file(rss_info['title']):
                    if self.__touch_strm_file(file_name=rss_info['title'], 
                                              file_url=rss_info['link'], # 直接传递 RSS link 作为 .strm 内容
                                              season=rss_info['season'], 
                                              sub_paths=rss_info['path_parts'], # 传递 sub_paths，但 __touch_strm_file 不再用它来构建本地目录
                                              overwrite=overwrite_mode):
                        cnt += 1
        
        logger.info(f'任务完成。共创建了 {cnt} 个新的 .strm 文件。')

    def get_state(self) -> bool:
        return self._enabled

    def get_command(self) -> List[Dict[str, Any]]:
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        pass

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                'component': 'VForm',
                'content': [
                    {
                        'component': 'VRow',
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 3}, 'content': [{'component': 'VSwitch', 'props': {'model': 'enabled', 'label': '启用插件'}}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 3}, 'content': [{'component': 'VSwitch', 'props': {'model': 'onlyonce', 'label': '立即运行一次'}}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 3}, 'content': [{'component': 'VSwitch', 'props': {'model': 'fulladd', 'label': '创建当季所有番剧'}}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 3}, 'content': [{'component': 'VSwitch', 'props': {'model': 'allseason', 'label': '补全历史所有番剧'}}]}
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [{'component': 'VTextField', 'props': {'model': 'cron', 'label': '执行周期 (Cron)', 'placeholder': '例如: 0 22 * * *'}}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [{'component': 'VTextField', 'props': {'model': 'storageplace', 'label': 'Strm存储路径', 'placeholder': '/downloads/strm'}}]}
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [{'component': 'VSwitch', 'props': {'model': 'overwrite', 'label': '强制覆盖已存在的Strm文件'}}]} 
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
            "overwrite": False, 
        }

    def __update_config(self):
        self.update_config({
            "onlyonce": self._onlyonce,
            "cron": self._cron,
            "enabled": self._enabled,
            "fulladd": self._fulladd,
            "allseason": self._allseason,
            "storageplace": self._storageplace,
            "overwrite": self._overwrite, 
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
            logger.error("退出插件失败：%s" % str(e))

if __name__ == "__main__":
    class MockLogger:
        def info(self, msg): print(f"信息: {msg}")
        def warn(self, msg): print(f"警告: {msg}")
        def error(self, msg): print(f"错误: {msg}")
        def debug(self, msg): print(f"调试: {msg}")

    logger = MockLogger()
    
    anistrm100 = ANiStrm100()
    anistrm100._storageplace = "./strm_test_cn"
    anistrm100.settings = lambda: None
    anistrm100.settings.USER_AGENT = "Mozilla/5.0"
    anistrm100.settings.PROXY = None
    
    print("--- 测试 get_all_season_list (起始年份2019，包含 'ANi' 目录) ---")
    all_files = anistrm100.get_all_season_list() 
    print(f"--- 总共找到 {len(all_files)} 个文件 ---")

    for i, (season, path_parts, file_name) in enumerate(all_files[:3]):
        print(f"[{i+1}] 原始信息：根目录: {season}, 子路径: {'/'.join(path_parts) if path_parts else '无'}, 文件: {file_name}")
    if len(all_files) > 3:
        print("...")
        for i, (season, path_parts, file_name) in enumerate(all_files[-3:], start=len(all_files)-2):
             print(f"[{i}] 原始信息：根目录: {season}, 子路径: {'/'.join(path_parts) if path_parts else '无'}, 文件: {file_name}")

    print("\n--- 模拟任务运行 (allseason模式) ---")
    anistrm100._overwrite = True 
    anistrm100.__task(allseason=True)
    
    print("\n--- 模拟任务运行 (RSS模式，不覆盖) ---")
    anistrm100._overwrite = False 
    anistrm100.__task(allseason=False, fulladd=False)
