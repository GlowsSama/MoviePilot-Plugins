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
