import pandas as pd
import numpy as np

def calculate_float_market_value():
    """
    计算流通市值 = 成交额 / 换手率
    并保存回原文件
    """
    # 读取数据
    print("正在读取数据...")
    df = pd.read_parquet('dataset/all_stocks.parquet')
    
    print(f"数据形状: {df.shape}")
    print(f"列名: {df.columns.tolist()}")
    
    # 检查必要的列是否存在
    required_columns = ['成交额', '换手率']
    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"缺少必要的列: {col}")
    
    # 计算流通市值
    print("正在计算流通市值...")
    # 避免除零错误，将换手率为0的行设为NaN
    df['流通市值'] = np.where(df['换手率'] != 0, 
                           df['成交额'] / df['换手率'], 
                           np.nan)
    
    # 显示计算结果统计
    print(f"流通市值统计:")
    print(f"  非空值数量: {df['流通市值'].notna().sum()}")
    print(f"  空值数量: {df['流通市值'].isna().sum()}")
    print(f"  最小值: {df['流通市值'].min()}")
    print(f"  最大值: {df['流通市值'].max()}")
    print(f"  平均值: {df['流通市值'].mean()}")
    
    # 保存回原文件
    print("正在保存数据...")
    df.to_parquet('dataset/all_stocks.parquet', index=True)
    print("数据已成功保存！")
    
    # 显示前几行结果
    print("\n前5行数据（包含新计算的流通市值）:")
    print(df[['成交额', '换手率', '流通市值']].head())

if __name__ == "__main__":
    calculate_float_market_value()
