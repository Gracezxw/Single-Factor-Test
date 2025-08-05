import numpy as np
import pandas as pd

def calculate_factor_MOM_vol_adj(df: pd.DataFrame) -> pd.Series:
    """
    计算动量因子 MOM_vol_adj
    公式: R_{t-N→t}/σ_{t-N→t}
    其中 R_{t-N→t} = (P_t - P_{t-N}) / P_{t-N}
    σ_{t-N→t} 是过去N天收盘价的标准差
    
    Parameters:
    -----------
    df : pd.DataFrame
        包含价格数据的DataFrame
        
    Returns:
    --------
    pd.Series
        动量因子值
    """
    
    # 回看期数
    N = 3

    price_col = '收盘'
    group_col = '股票代码'
    
    prices = df[price_col].values
    groups = df[group_col].values
    
    # 找到分组边界
    group_changes = np.where(groups[1:] != groups[:-1])[0] + 1
    group_starts = np.concatenate([[0], group_changes])
    group_ends = np.concatenate([group_changes, [len(groups)]])
    
    # 初始化结果数组
    factor_values = np.full_like(prices, np.nan, dtype=np.float64)
    
    for start, end in zip(group_starts, group_ends):
        if end - start > N:  # 确保有足够的数据
            group_prices = prices[start:end]
            
            # 计算分子: R_{t-N→t} = (P_t - P_{t-N}) / P_{t-N}
            returns = np.full_like(group_prices, np.nan, dtype=np.float64)
            returns[N:] = (group_prices[N:] - group_prices[:-N]) / group_prices[:-N]
            
            # 计算分母: σ_{t-N→t} 是过去N天收盘价的标准差
            std_values = np.full_like(group_prices, np.nan, dtype=np.float64)
            
            for i in range(N, len(group_prices)):
                # 计算过去N天的标准差
                past_N_prices = group_prices[i-N:i]
                std_values[i] = np.std(past_N_prices)
            
            # 计算因子值: R_{t-N→t}/σ_{t-N→t}
            factor_values[start+N:end] = returns[N:] / std_values[N:]
    
    return pd.Series(factor_values, index=df.index, name=f'MOM_vol_adj_3D')
    