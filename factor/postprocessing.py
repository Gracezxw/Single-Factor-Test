import pandas as pd
import numpy as np
import statsmodels.api as sm
import warnings

warnings.filterwarnings("ignore", category=FutureWarning)

def standardize_factor(factor_data: pd.DataFrame, n: int = 3):
    """
    对因子数据进行标准化处理
    
    Args:
        factor_data: 包含因子数据的DataFrame，需要包含日期列
        n: 中位数方法中的参数，默认为3
    
    Returns:
        标准化后的因子DataFrame
    """
    # 复制数据避免修改原始数据
    data = factor_data.copy()
    
    # 获取所有因子列
    factor_columns = ["MOM_vol_adj_3D"]
    
    def median_method(series, n=3):
        """
        中位数标准化方法
        
            计算序列的中位数 xm 和中位数绝对偏差 Dmad.
            设置上下界：xm ± n * Dmad.
            对超出范围的值进行截断处理.
            最后进行标准化：(adjusted - mean) / std.
        """
        arr = series.values
        xm = np.median(arr)
        dmad = np.median(np.abs(arr - xm))
        
        upper_bound = xm + n * dmad
        lower_bound = xm - n * dmad
        adjusted = np.clip(arr, lower_bound, upper_bound)
        
        standardized = (adjusted - np.mean(adjusted)) / np.std(adjusted)
        return pd.Series(standardized, index=series.index)

    def rank_method(series):
        """
        排序标准化方法
        
            将每个值转换为其在序列中的排名
            对排名进行标准化：(rank - mean) / std.
        """
        ranks = series.rank()
        standardized = (ranks - ranks.mean()) / ranks.std()
        return standardized
    
    # 对每个因子应用两种标准化方法
    for factor in factor_columns:
        if factor not in data.columns:
            print(f"Warning: Factor {factor} not found in data")
            continue
            
        # 中位数方法
        data[f"{factor}.median_std"] = data.groupby('日期')[factor].transform(
            lambda x: median_method(x, n)
        )
        
        # 排序方法
        data[f"{factor}.rank_std"] = data.groupby('日期')[factor].transform(
            lambda x: rank_method(x)
        )
    
    print("标准化完成。新增列:")
    print([col for col in data.columns if '.median_std' in col or '.rank_std' in col])
    
    return data


def neutralize_factor(factor_data: pd.DataFrame):
    """
    对因子数据进行中性化处理
    
    将因子值对行业和市值进行回归，取残差作为中性化后的因子值
    
    Args:
        factor_data: 包含因子数据的DataFrame，需要包含以下列：
            - 日期: 交易日期
            - 股票代码: 股票代码
            - 流通市值: 市值
            - 行业分类列（以行业名称为前缀的列）
            - 因子列（以.median_std或.rank_std结尾的列）
    
    Returns:
        中性化后的因子DataFrame
    """
    # 复制数据避免修改原始数据
    data = factor_data.copy()
    
    # 识别行业列
    industry_cols = ["行业"]
    
    if not industry_cols:
        raise ValueError("未找到行业分类列")
    
    # 检查必要的列
    required_cols = ['日期', '流通市值']
    missing_cols = [col for col in required_cols if col not in data.columns]
    if missing_cols:
        raise ValueError(f"缺少必要的列: {missing_cols}")
    
    # 准备市值数据
    data['log_mcap'] = np.log(data['流通市值'].astype(np.float64))
    
    # 获取因子列（以.median_std或.rank_std结尾的列）
    factor_columns = [col for col in data.columns if col.endswith('.median_std') or col.endswith('.rank_std')]
    
    if not factor_columns:
        raise ValueError("未找到标准化后的因子列")
    
    def safe_daily_regression(y: pd.Series, X: pd.DataFrame):
        """
        安全的每日回归计算
        
        Args:
            y: 因变量（因子值）
            X: 自变量（行业哑变量和市值）
        
        Returns:
            回归残差，如果回归失败则返回None
        """
        try:
            # 强制类型转换
            X = X.astype(np.float64)
            y = y.astype(np.float64)
            
            # 检查有效样本量
            if len(y) < 10 or X.shape[1] >= len(y):
                return None
                
            # 添加截距项
            X = sm.add_constant(X, has_constant='raise')
            
            # 矩阵秩检查
            if np.linalg.matrix_rank(X) < X.shape[1]:
                return None
                
            model = sm.OLS(y, X).fit()
            return model.resid
            
        except Exception as e:
            print(f"回归失败: {str(e)}")
            return None
    
    def daily_cross_sectional_neutralize(factor_series: pd.Series):
        """
        日频横截面中性化
        
        Args:
            factor_series: 因子序列
        
        Returns:
            中性化后的因子序列
        """
        neutralized = pd.Series(np.nan, index=factor_series.index, name=factor_series.name)
        
        for date, daily_data in data.groupby('日期'):
            try:
                # 准备行业哑变量
                industries = daily_data[industry_cols].idxmax(axis=1)
                industry_dummies = pd.get_dummies(industries, drop_first=True)
                
                # 构建设计矩阵
                X = pd.concat([
                    industry_dummies.astype(np.float64),
                    daily_data['log_mcap'].astype(np.float64)
                ], axis=1)
                
                # 对齐因子值
                y = factor_series.loc[daily_data.index]
                valid_idx = y.notna() & X.notna().all(axis=1)
                
                # 执行安全回归
                residuals = safe_daily_regression(
                    y=y[valid_idx],
                    X=X[valid_idx]
                )
                
                if residuals is not None:
                    neutralized.loc[daily_data.index[valid_idx]] = residuals
                    
            except Exception as e:
                print(f"处理日期 {date} 时出错: {str(e)}")
                continue
                
        return neutralized
    
    # 对每个因子进行中性化处理
    for factor_col in factor_columns:
        print(f"\n处理因子: {factor_col}")
        
        # 中性化处理
        neutralized_col = f"{factor_col}_neutral"
        data[neutralized_col] = daily_cross_sectional_neutralize(data[factor_col])
        
        # 标准化
        data[neutralized_col] = (
            (data[neutralized_col] - data[neutralized_col].mean()) 
            / data[neutralized_col].std()
        )
        
        # 验证结果
        valid_pct = data[neutralized_col].notna().mean() * 100
        print(f"中性化完成，有效数据比例: {valid_pct:.1f}%")
    
    print("\n中性化完成。新增列:")
    print([col for col in data.columns if '_neutral' in col])
    
    return data