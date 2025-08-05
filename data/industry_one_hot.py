# -*- coding: utf-8 -*-
"""
行业独热编码模块
将数据集中的行业列转换为每个行业的0/1列
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def load_dataset(file_path="dataset/all_stocks.parquet"):
    """
    加载数据集
    
    Args:
        file_path (str): 数据集文件路径
        
    Returns:
        pd.DataFrame: 加载的数据集
    """
    try:
        logger.info(f"正在加载数据集: {file_path}")
        df = pd.read_parquet(file_path)
        logger.info(f"数据集加载成功，形状: {df.shape}")
        return df
    except Exception as e:
        logger.error(f"加载数据集失败: {e}")
        raise

def get_industry_categories(df):
    """
    获取所有行业类别
    
    Args:
        df (pd.DataFrame): 数据集
        
    Returns:
        list: 行业类别列表
    """
    if '行业' not in df.columns:
        raise ValueError("数据集中没有找到'行业'列")
    
    # 获取所有唯一的行业类别，排除空值和'-'
    industries = df['行业'].dropna().unique()
    industries = [ind for ind in industries if ind != '-' and ind != '']
    industries.sort()  # 排序以保持一致性
    
    logger.info(f"发现 {len(industries)} 个行业类别")
    return industries

def create_industry_one_hot(df, industries=None):
    """
    创建行业独热编码
    
    Args:
        df (pd.DataFrame): 原始数据集
        industries (list, optional): 行业类别列表，如果为None则自动获取
        
    Returns:
        pd.DataFrame: 包含独热编码的新数据集
    """
    if industries is None:
        industries = get_industry_categories(df)
    
    # 创建独热编码
    one_hot_df = df.copy()
    
    # 为每个行业创建0/1列
    for industry in industries:
        # 创建行业列名（处理特殊字符）
        col_name = f"行业_{industry}"
        # 创建0/1列
        one_hot_df[col_name] = (df['行业'] == industry).astype(int)
    
    logger.info(f"已创建 {len(industries)} 个行业独热编码列")
    
    return one_hot_df

def save_industry_one_hot(df, output_path="dataset/all_stocks_industry_one_hot.parquet"):
    """
    保存包含行业独热编码的数据集
    
    Args:
        df (pd.DataFrame): 包含独热编码的数据集
        output_path (str): 输出文件路径
    """
    try:
        # 确保输出目录存在
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        
        # 保存数据集
        df.to_parquet(output_path, index=True)
        logger.info(f"数据集已保存到: {output_path}")
        
    except Exception as e:
        logger.error(f"保存数据集失败: {e}")
        raise

def process_industry_one_hot(input_path="dataset/all_stocks.parquet", 
                           output_path="dataset/all_stocks_industry_one_hot.parquet",
                           save_original_industry=True):
    """
    处理行业独热编码的主函数
    
    Args:
        input_path (str): 输入数据集路径
        output_path (str): 输出数据集路径
        save_original_industry (bool): 是否保留原始行业列
        
    Returns:
        pd.DataFrame: 处理后的数据集
    """
    # 加载数据集
    df = load_dataset(input_path)
    
    # 获取行业类别
    industries = get_industry_categories(df)
    
    # 创建独热编码
    one_hot_df = create_industry_one_hot(df, industries)
    
    # 如果不保留原始行业列，则删除
    if not save_original_industry:
        one_hot_df = one_hot_df.drop(columns=['行业'])
        logger.info("已删除原始行业列")
    
    # 保存数据集
    save_industry_one_hot(one_hot_df, output_path)
    
    # 打印统计信息
    print("\n=== 行业独热编码统计信息 ===")
    print(f"原始数据集形状: {df.shape}")
    print(f"处理后数据集形状: {one_hot_df.shape}")
    print(f"新增行业独热编码列数: {len(industries)}")
    print(f"行业类别列表:")
    for i, industry in enumerate(industries, 1):
        print(f"  {i:2d}. {industry}")
    
    return one_hot_df

def verify_one_hot_encoding(df, industries):
    """
    验证独热编码的正确性
    
    Args:
        df (pd.DataFrame): 包含独热编码的数据集
        industries (list): 行业类别列表
        
    Returns:
        bool: 验证是否通过
    """
    logger.info("正在验证独热编码...")
    
    # 检查每个样本的独热编码列之和是否为1（对于有行业信息的样本）
    industry_cols = [f"行业_{ind}" for ind in industries]
    
    # 只检查有行业信息的行
    valid_rows = df['行业'].notna() & (df['行业'] != '-')
    
    if valid_rows.sum() > 0:
        # 计算每行的独热编码列之和
        row_sums = df.loc[valid_rows, industry_cols].sum(axis=1)
        
        # 检查是否所有有效行都只有一个行业被标记为1
        if not (row_sums == 1).all():
            logger.error("独热编码验证失败：某些样本的行业编码不正确")
            return False
        
        logger.info("独热编码验证通过")
        return True
    else:
        logger.warning("没有找到有效的行业信息进行验证")
        return True

if __name__ == "__main__":
    # 主程序入口
    try:
        # 处理行业独热编码
        result_df = process_industry_one_hot()
        
        # 验证结果
        industries = get_industry_categories(result_df)
        verify_one_hot_encoding(result_df, industries)
        
        print("\n✅ 行业独热编码处理完成！")
        
    except Exception as e:
        logger.error(f"处理失败: {e}")
        raise
