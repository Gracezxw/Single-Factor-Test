import pandas as pd
import os
import glob
from pathlib import Path

def merge_all_stocks_data():
    """
    合并dataset文件夹中的所有股票CSV文件，并转换为parquet格式
    """
    # 设置路径
    dataset_path = Path("dataset")
    output_path = Path("dataset")
    
    # 确保输出目录存在
    output_path.mkdir(exist_ok=True)
    
    # 获取所有CSV文件
    csv_files = glob.glob(str(dataset_path / "*.csv"))
    
    if not csv_files:
        print("未找到CSV文件")
        return
    
    print(f"找到 {len(csv_files)} 个CSV文件")
    
    # 存储所有数据框的列表
    all_dataframes = []
    
    # 读取每个CSV文件
    for i, csv_file in enumerate(csv_files):
        try:
            # 读取CSV文件
            df = pd.read_csv(csv_file)
            
            # 如果股票代码列不存在或为空，则跳过
            if '股票代码' not in df.columns or df['股票代码'].isna().all():
                continue
            
            all_dataframes.append(df)
            
            if (i + 1) % 100 == 0:
                print(f"已处理 {i + 1}/{len(csv_files)} 个文件")
                
        except Exception as e:
            print(f"处理文件 {csv_file} 时出错: {e}")
            continue
    
    if not all_dataframes:
        print("没有成功读取任何数据")
        return
    
    # 合并所有数据框
    print("正在合并数据...")
    merged_df = pd.concat(all_dataframes, ignore_index=True)
    
    # 按日期和股票代码排序
    if '日期' in merged_df.columns:
        merged_df['日期'] = pd.to_datetime(merged_df['日期'])
        merged_df = merged_df.sort_values(['日期', '股票代码'])
    
    # 设置多级索引（如果需要）
    if '日期' in merged_df.columns and '股票代码' in merged_df.columns:
        merged_df.set_index(['股票代码', '日期'], inplace=True)
    
    # 保存为parquet格式
    output_file = output_path / "all_stocks.parquet"
    print(f"正在保存到 {output_file}...")
    merged_df.to_parquet(output_file, index=True)
    
    print(f"合并完成！")
    print(f"总行数: {len(merged_df)}")
    print(f"股票数量: {merged_df.index.get_level_values('股票代码').nunique()}")
    print(f"日期范围: {merged_df.index.get_level_values('日期').min()} 到 {merged_df.index.get_level_values('日期').max()}")
    print(f"文件大小: {output_file.stat().st_size / (1024*1024):.2f} MB")

if __name__ == "__main__":
    merge_all_stocks_data()
