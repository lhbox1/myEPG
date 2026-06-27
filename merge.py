import xml.etree.ElementTree as ET
from collections import defaultdict
import aiohttp
import asyncio
from tqdm.asyncio import tqdm_asyncio
from datetime import datetime, timezone, timedelta
import gzip
import shutil
from xml.dom import minidom
import re
from opencc import OpenCC
import os
from tqdm import tqdm

TZ_UTC_PLUS_8 = timezone(timedelta(hours=8))

# 【优化 3】将 OpenCC 提为全局变量，避免几十万次重复初始化导致的性能崩溃
CC_T2S = OpenCC("t2s")

def transform2_zh_hans(string):
    if not string:
        return ""
    return CC_T2S.convert(string)

# ============ EPG 源预处理规则 ============
def _adjust_timezone(programme, from_offset, to_offset):
    for attr in ('start', 'stop'):
        val = programme.get(attr, '')
        if from_offset in val:
            programme.set(attr, val.replace(from_offset, to_offset))

def _make_tz_rule(channel_keyword, from_offset, to_offset):
    def rule(channel_name, programme):
        if channel_keyword in channel_name:
            _adjust_timezone(programme, from_offset, to_offset)
    return rule

PREPROCESS_RULES = [
    ("kuke31/xmlgz", _make_tz_rule("天映经典", "+0800", "+0700")),
]

def preprocess_epg(url, epg_content):
    matched_rules = [rule for keyword, rule in PREPROCESS_RULES if keyword in url]
    if not matched_rules:
        return epg_content

    try:
        parser = ET.XMLParser(encoding='UTF-8')
        root = ET.fromstring(epg_content, parser=parser)
    except ET.ParseError:
        return epg_content

    channel_names = {}
    for channel in root.findall('channel'):
        cid = channel.get('id', '')
        names = [n.text for n in channel.findall('display-name') if n.text]
        channel_names[cid] = ' '.join(names) + ' ' + cid

    for programme in root.findall('programme'):
        cid = programme.get('channel', '')
        name_str = channel_names.get(cid, cid)
        for rule in matched_rules:
            rule(name_str, programme)

    return ET.tostring(root, encoding='unicode')
# ============ 预处理规则结束 ============

async def fetch_epg(url):
    connector = aiohttp.TCPConnector(limit=16, ssl=False)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36"
    }
    try:
        async with aiohttp.ClientSession(connector=connector, trust_env=True, headers=headers) as session:
            async with session.get(url) as response:
                if url.endswith('.gz'):
                    compressed_data = await response.read()
                    return gzip.decompress(compressed_data).decode('utf-8', errors='ignore')
                else:
                    return await response.text(encoding='utf-8')
    except Exception as e:
        print(f"{url} 请求错误: {e}")
    return None

def process_display_name(display_name):
    if display_name.endswith('高清'):
        display_name = display_name[:-2]
    return display_name

def parse_epg(epg_content):
    try:
        parser = ET.XMLParser(encoding='UTF-8')
        root = ET.fromstring(epg_content, parser=parser)
    except ET.ParseError as e:
        print(f"Error parsing XML: {e}")
        return {}, defaultdict(list)

    channels = {}
    programmes = defaultdict(list)

    for channel in root.findall('channel'):
        channel_id = transform2_zh_hans(channel.get('id'))
        channel_display_names = []
        for name in channel.findall('display-name'):
            t_name = transform2_zh_hans(name.text)
            t_name = process_display_name(t_name)
            channel_display_names.append([t_name, name.get('lang', 'zh')])
        if not channel_id.isdigit() and channel_id not in [x[0] for x in channel_display_names]:
            channel_display_names.append([channel_id, 'zh'])
        channels[channel_id] = channel_display_names

    today = datetime.now(TZ_UTC_PLUS_8).date()
    valid_channels = set()

    for programme in root.findall('programme'):
        channel_id = transform2_zh_hans(programme.get('channel'))
        try:
            channel_start = datetime.strptime(re.sub(r'\s+', '', programme.get('start')), "%Y%m%d%H%M%S%z").astimezone(TZ_UTC_PLUS_8)
            channel_stop = datetime.strptime(re.sub(r'\s+', '', programme.get('stop')), "%Y%m%d%H%M%S%z").astimezone(TZ_UTC_PLUS_8)
        except Exception:
            continue

        # 【优化 2】改善过滤机制：只要节目涵盖今天或未来，该频道即为有效
        if channel_stop.date() >= today:
            valid_channels.add(channel_id)

        # 【优化 1】修复对 root 节点的动态污染，改用纯净的独立 Element 创生
        channel_elem = ET.Element('programme', attrib={
            "start": channel_start.strftime("%Y%m%d%H%M%S %z"), 
            "stop": channel_stop.strftime("%Y%m%d%H%M%S %z")
        })
        
        for title in programme.findall('title'):
            channel_title = "精彩节目" if title.text is None else title.text.strip()
            langattr = title.get('lang')
            if langattr == 'zh' or langattr is None:
                channel_title = transform2_zh_hans(channel_title)
            channel_elem_t = ET.SubElement(channel_elem, 'title')
            channel_elem_t.text = channel_title
            if langattr is not None:
                channel_elem_t.set('lang', langattr)
                
        for desc in programme.findall('desc'):
            if desc.text is None:
                continue
            langattr = desc.get('lang')
            channel_desc = desc.text.strip()
            if langattr == 'zh' or langattr is None:
                channel_desc = transform2_zh_hans(channel_desc)
            channel_elem_d = ET.SubElement(channel_elem, 'desc')
            channel_elem_d.text = channel_desc
            if langattr is not None:
                channel_elem_d.set('lang', langattr)
                
        programmes[channel_id].append(channel_elem)
        
    channels = {k: v for k, v in channels.items() if k in valid_channels}
    programmes = {k: v for k, v in programmes.items() if k in valid_channels}

    return channels, programmes

def write_to_xml(all_channel_id, all_channel_names, programmes, filename):
    if not os.path.exists('output'):
        os.makedirs('output')
    current_time = datetime.now(TZ_UTC_PLUS_8).strftime("%Y%m%d%H%M%S %z")
    root = ET.Element('tv', attrib={'date': current_time})
    
    for channel_id in all_channel_id:
        channel_elem = ET.SubElement(root, 'channel', attrib={"id": channel_id})
        for display_name_node in all_channel_names[channel_id]:
            display_name = display_name_node[0]
            langattr = display_name_node[1]
            display_name_elem = ET.SubElement(channel_elem, 'display-name', attrib={"lang": langattr})
            display_name_elem.text = display_name
        for prog in programmes[channel_id]:
            prog.set('channel', channel_id)
            root.append(prog)

    rough_string = ET.tostring(root, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(reparsed.toprettyxml(indent='\t', newl='\n'))

def compress_to_gz(input_filename, output_filename):
    with open(input_filename, 'rb') as f_in:
        with gzip.open(output_filename, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

def get_urls():
    urls = []
    if not os.path.exists('config.txt'):
        return urls
    with open('config.txt', 'r', encoding='utf-8') as file:
        for line in file:
            line = line.strip()
            if line and not line.startswith('#'):
                urls.append(line)
    return urls

async def main():
    urls = get_urls()
    if not urls:
        print("No URLs found in config.txt.")
        return
    tasks = [fetch_epg(url) for url in urls]
    print("Fetching EPG data...")
    epg_contents = await tqdm_asyncio.gather(*tasks, desc="Fetching URLs")
    
    all_channels_map = {}
    all_channel_id = set()
    all_channel_names = defaultdict(list)
    all_programmes = defaultdict(list)
    
    print("Finished Fetching. Parsing and Merging...")
    for idx, epg_content in enumerate(epg_contents):
        if epg_content is None:
            continue
        url = urls[idx]
        epg_content = preprocess_epg(url, epg_content)
        channels, programmes = parse_epg(epg_content)
        
        with tqdm(total=len(channels), desc=f"Source {idx+1}/{len(epg_contents)}", unit="ch") as pbar:
            for channel_id, display_names in channels.items():
                if len(programmes[channel_id]) == 0:
                    pbar.update(1)
                    continue
                
                is_in_map = False
                target_id = channel_id
                
                for display_name_node in display_names:
                    display_name = display_name_node[0]
                    if display_name in all_channels_map:
                        is_in_map = True
                        target_id = all_channels_map[display_name]
                        break
                
                if not is_in_map:
                    all_channel_id.add(target_id)
                    all_channel_names[target_id] = display_names
                    all_programmes[target_id] = programmes[channel_id]
                    for display_name_node in display_names:
                        all_channels_map[display_name_node[0]] = target_id
                else:
                    if len(all_programmes[target_id]) < len(programmes[channel_id]):
                        all_programmes[target_id] = programmes[channel_id]
                    for display_name_node in display_names:
                        dn = display_name_node[0]
                        if dn not in all_channels_map:
                            all_channel_names[target_id].append(display_name_node)
                            all_channels_map[dn] = target_id
                pbar.update(1)
                
    print("Writing to XML...")
    write_to_xml(all_channel_id, all_channel_names, all_programmes, 'output/epg.xml')
    compress_to_gz('output/epg.xml', 'output/epg.gz')
    print("All tasks completed successfully!")

if __name__ == '__main__':
    asyncio.run(main())
