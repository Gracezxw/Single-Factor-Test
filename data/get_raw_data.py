# -*- coding: utf-8 -*-
import akshare as ak
import pandas as pd
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from tqdm import tqdm
from pathlib import Path
import sys

# 设置日志 - 精简输出
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def get_exchange_by_code(stock_code):
    """根据股票代码判断交易所"""
    # 处理NaN值
    if pd.isna(stock_code):
        return 'Unknown'
    
    # 转换为字符串并去除空格
    code = str(stock_code).strip()
    
    # 处理空字符串
    if not code or code == 'nan':
        return 'Unknown'
    
    # 处理带前缀的代码
    if code.startswith(('bj', 'sz', 'sh')):
        if code.startswith('bj') or code.startswith('sz'):
            return 'SZSE'
        elif code.startswith('sh'):
            return 'SSE'
        code = code[2:]
    
    # 判断交易所
    if code.startswith(('60', '68', '900')):
        return 'SSE'  # 上交所
    elif code.startswith(('000', '002', '300', '200')):
        return 'SZSE'  # 深交所
    else:
        return 'Unknown'

def get_stock_list(force_update=False, start_index=0):
    """获取股票列表，按交易所分类，优先从本地缓存读取"""
    cache_file = "dataset/stock_list_cache.csv"
    
    if not os.path.exists(cache_file) or force_update:
        try:
            if force_update:
                logger.info("强制更新股票列表...")
            else:
                logger.info("从网络获取股票列表...")
            # 获取A股实时行情数据
            df = ak.stock_zh_a_spot_em()
        
            # 保存到本地缓存
            df.to_csv(cache_file, index=False, encoding='utf-8-sig')
            logger.info(f"股票列表已保存到本地缓存: {cache_file}")
        
        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
            return [], []
    
    # 检查本地缓存是否存在且不强制更新
    else:
        try:
            logger.info("从本地缓存读取股票列表...")
            df = pd.read_csv(cache_file, encoding='utf-8-sig')
            
        except Exception as e:
            logger.warning(f"读取本地缓存失败: {e}")
            
    # 添加交易所列
    df['交易所'] = df['代码'].apply(get_exchange_by_code)
    
    # 过滤有效的股票代码
    valid_df = df[df['交易所'].isin(['SSE', 'SZSE'])]
    
    # 从指定索引开始选择股票
    if start_index > 0:
        logger.info(f"从第 {start_index} 项开始选择股票...")
        valid_df = valid_df.iloc[start_index:]
    
    # 按交易所分组并选择前500支
    # sse_stocks = valid_df[valid_df['交易所'] == 'SSE']['代码'].head(500).tolist()
    # szse_stocks = valid_df[valid_df['交易所'] == 'SZSE']['代码'].head(500).tolist()
    
    sse_stocks = valid_df[valid_df['交易所'] == 'SSE']['代码'].tolist()
    szse_stocks = valid_df[valid_df['交易所'] == 'SZSE']['代码'].tolist()
    
    logger.info(f"从缓存获取到上交所股票: {len(sse_stocks)}支")
    logger.info(f"从缓存获取到深交所股票: {len(szse_stocks)}支")
    
    return sse_stocks, szse_stocks
        

def download_stock_data(stock_code, start_date="20100101", end_date="20201231", output_dir="stock_data", max_retries=3):
    """下载单支股票的历史数据，带重试机制"""
    # 确保股票代码是字符串类型
    if pd.isna(stock_code):
        return stock_code, False, 0
    
    stock_code_str = str(stock_code).strip()
    if not stock_code_str or stock_code_str == 'nan':
        return stock_code, False, 0
    
    for attempt in range(max_retries):
        try:
            # 创建输出目录
            os.makedirs(output_dir, exist_ok=True)
            
            # 添加5秒延迟，防止被封IP
            time.sleep(2)
            
            # 获取股票历史数据
            df = ak.stock_zh_a_hist(
                symbol=stock_code_str, 
                period="daily", 
                start_date=start_date, 
                end_date=end_date, 
                adjust="hfq"
            )
            
            if df is not None and not df.empty:
                # 检查数据完整性：2010-2020年数据是否完整
                df['日期'] = pd.to_datetime(df['日期'])
                start_year = df['日期'].dt.year.min()
                end_year = df['日期'].dt.year.max()
                
                # 检查是否覆盖2010-2020年
                if start_year <= 2010 and end_year >= 2020:
                    # 保存为CSV文件
                    csv_file = os.path.join(output_dir, f"{stock_code}.csv")
                    df.to_csv(csv_file, index=False, encoding='utf-8-sig')
                    
                    return stock_code, True, len(df)
                else:
                    return stock_code, False, 0
            else:
                return stock_code, False, 0
                
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"下载股票 {stock_code} 失败 (尝试 {attempt + 1}/{max_retries}): {e}")
                time.sleep(10)  # 重试前等待10秒
            else:
                logger.error(f"下载股票 {stock_code} 数据最终失败: {e}")
                return stock_code, False, 0
    
    return stock_code, False, 0

def download_batch_data(stock_list, start_date="20100101", end_date="20201231", output_dir="stock_data", max_workers=1):
    """串行下载股票数据，避免限流"""
    results = []
    
    # 使用tqdm显示进度
    with tqdm(total=len(stock_list), desc="下载进度") as pbar:
        for stock_code in stock_list:
            # 统计跳过的股票（数据已存在且完整）
            csv_file = os.path.join(output_dir, f"{stock_code}.csv")
            if os.path.exists(csv_file):
                # 如果文件已存在，跳过下载
                pbar.update(1)
                continue
            
            stock_code, success, count = download_stock_data(stock_code, start_date, end_date, output_dir)
            results.append((stock_code, success, count))
            pbar.update(1)
    
    return results

def get_stock_info(symbol, info_type: str):
    """
    获取股票的个股信息，如股票简称、总股本、流通股、总市值、流通市值、行业、上市时间等.
    """
    try:
        # 使用akshare获取股票信息
        stock_info = ak.stock_individual_info_em(symbol=symbol)
        
        # 查找行业信息
        info_row = stock_info[stock_info['item'] == info_type]
        if not info_row.empty:
            return info_row.iloc[0]['value']
        else:
            return "Unknown"
    except Exception as e:
        print(f"获取股票 {symbol} {info_type} 信息失败: {e}")
        return "Unknown"

def add_info_to_stock_data():
    """
    为dataset文件夹中的每支股票数据添加行业信息
    """
    # 设置路径
    dataset_path = Path("dataset")
    
    # 获取所有CSV文件
    csv_files = list(dataset_path.glob("*.csv"))
    
    # 过滤掉非股票数据文件
    stock_files = [f for f in csv_files if f.name != "stock_list_cache.csv" and f.name != "successful_stocks.csv"]
    
    print(f"找到 {len(stock_files)} 个股票数据文件")
    
    # 处理每个股票文件
    for i, file_path in enumerate(stock_files):
        try:
            # 从文件名获取股票代码
            stock_code = file_path.stem
            
            print(f"处理第 {i+1}/{len(stock_files)} 个文件: {stock_code}")
            
            # 读取股票数据
            df = pd.read_csv(file_path)
            
            # 检查是否已经有总市值列
            if '总市值' in df.columns:
                print(f"股票 {stock_code} 已有总市值信息，跳过")
                continue
            
            # 获取行业信息
            industry = get_stock_info(stock_code, "行业")
            
            # 添加行业列
            df['行业'] = industry
            
            # 检查是否已经有行业列
            if '行业' in df.columns:
                print(f"股票 {stock_code} 已有行业信息，跳过")
                continue
            
            # 获取行业信息
            industry = get_stock_info(stock_code, "行业")
            
            # 添加行业列
            df['行业'] = industry
            
            # 保存更新后的数据
            df.to_csv(file_path, index=False)
            
            print(f"股票 {stock_code} 行业信息已添加: {industry}")
            
            # 添加延时避免请求过于频繁
            time.sleep(0.5)
            
        except Exception as e:
            print(f"处理股票 {stock_code} 时出错: {e}")
            continue
        
def get_hist_data(force_update_stock_list=False, start_index=0):
    """获取股票历史数据"""
    logger.info("开始获取股票列表...")
    
    # 获取股票列表
    sse_stocks, szse_stocks = get_stock_list(force_update=force_update_stock_list, start_index=start_index)
    
    if not sse_stocks and not szse_stocks:
        logger.error("无法获取股票列表")
        return
    
    # 合并所有股票
    all_stocks = sse_stocks + szse_stocks
    logger.info(f"总共需要下载 {len(all_stocks)} 支股票的数据")
    
    # 设置输出目录
    output_dir = "dataset"
    
    # 批量下载数据，直到获得1000支数据完整的股票
    successful_stocks = []
    total_processed = 0
    
    logger.info("开始批量下载股票数据，筛选2010-2020年数据完整的股票...")
    
    # 分批处理，避免一次性处理太多
    batch_size = 20  # 减少批次大小
    for i in range(0, len(all_stocks), batch_size):
        batch_stocks = all_stocks[i:i+batch_size]
        
        results = download_batch_data(
            stock_list=batch_stocks,
            start_date="20100101",
            end_date="20201231",
            output_dir=output_dir,
            max_workers=1  # 串行下载，避免限流
        )
        
        # 统计成功下载的股票
        batch_successful = [r for r in results if r[1]]
        successful_stocks.extend([r[0] for r in batch_successful])
        total_processed += len(batch_stocks)
        
        logger.info(f"已处理 {total_processed}/{len(all_stocks)} 支股票，成功获得 {len(successful_stocks)} 支数据完整的股票")
        
        
        # 如果已经获得足够的股票，停止下载
        if len(successful_stocks) >= 1000:
            logger.info(f"已获得 {len(successful_stocks)} 支股票，停止下载")
            break
    
    # 如果还不够1000支，继续处理剩余股票
    if len(successful_stocks) < 1000 and total_processed < 5409:
        remaining_stocks = all_stocks[total_processed:]
        logger.info(f"继续处理剩余 {len(remaining_stocks)} 支股票...")
        
        results = download_batch_data(
            stock_list=remaining_stocks,
            start_date="20100101",
            end_date="20201231",
            output_dir=output_dir,
            max_workers=1
        )
        
        batch_successful = [r for r in results if r[1]]
        successful_stocks.extend([r[0] for r in batch_successful])
    
    # 最终统计
    logger.info(f"下载完成！成功获得 {len(successful_stocks)} 支数据完整的股票")
    
    # 保存成功下载的股票列表
    successful_df = pd.DataFrame(successful_stocks, columns=['股票代码'])
    successful_df.to_csv(f"{output_dir}/successful_stocks.csv", index=False, encoding='utf-8-sig')
    
    logger.info(f"所有数据已保存到目录: {output_dir}")
    logger.info(f"成功下载的股票列表已保存到: {output_dir}/successful_stocks.csv")

if __name__ == "__main__":
    # 检查是否有强制更新参数
    force_update = "--force-update" in sys.argv or "-f" in sys.argv
    
    # 检查是否有起始索引参数
    start_index = 0
    for i, arg in enumerate(sys.argv):
        if arg == "--start-index" and i + 1 < len(sys.argv):
            try:
                start_index = int(sys.argv[i + 1])
            except ValueError:
                logger.error("起始索引必须是整数")
                sys.exit(1)
        elif arg.startswith("--start-index="):
            try:
                start_index = int(arg.split("=")[1])
            except ValueError:
                logger.error("起始索引必须是整数")
                sys.exit(1)
    
    if force_update:
        logger.info("检测到强制更新参数，将重新获取股票列表")
    
    if start_index > 0:
        logger.info(f"将从第 {start_index} 项开始下载股票数据")
    
    get_hist_data(force_update_stock_list=force_update, start_index=start_index)
    
    add_info_to_stock_data()