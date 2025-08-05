# Single-Factor-Test

## 安装依赖

```bash
pip install -r requirements.txt
```

## 使用方法

### 基本使用

```bash
python data.py
```

### 强制更新股票列表

```bash
python data.py --force-update
# 或者
python data.py -f
```

### 从指定索引开始下载

```bash
# 从第1271项开始下载
python data.py --start-index 1271

# 或者使用等号形式
python data.py --start-index=1271
```

## 缓存机制

程序会自动将股票列表保存在 `stock_list_cache.csv` 文件中：

1. **首次运行**: 从网络获取股票列表并保存到本地
2. **后续运行**: 直接从本地缓存读取，避免重复请求
3. **强制更新**: 使用 `--force-update` 参数可以强制重新获取股票列表

## 输出文件

- `dataset/stock_list_cache.csv`: 股票列表缓存
- `dataset/successful_stocks.csv`: 成功下载的股票列表
- `dataset/*.csv`: 各股票的历史数据文件

## 注意事项

- 程序会自动添加延迟避免被限流
- 建议在网络稳定的环境下运行
- 首次运行可能需要较长时间获取股票列表
