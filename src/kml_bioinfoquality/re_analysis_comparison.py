import click
import pandas as pd
from pathlib import Path
import re
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@click.command()
@click.option('--input-file', required=True,
              help='输入特定项目结果目录路径的文件, 逗号分隔, 逗号前为旧版本结果路径, 逗号后为新版本结果路径')
@click.option('--project-name', default='tcr', show_default=True, type=click.Choice(['tcr', 'lvis']), help='项目名称')
@click.option('--output-dir', required=True, help='输出结果目录')
@click.help_option('--help', help='显示帮助信息')
def main(input_file, project_name, output_dir):
    """重分析结果比较, 用于半年度重分析和流程变更后评估"""
    logging.debug(f"项目名称: {project_name}")
    logging.debug(f"输入文件: {input_file}")
    logging.debug(f"输出目录: {output_dir}")
    logging.info('开始比较重分析结果')

    output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True, parents=True)
    if project_name == 'tcr':
        tcr_comparison(input_file, output_dir)
    elif project_name == 'lvis':
        lvis_comparison(input_file, output_dir)

    logging.info('重分析结果比较完成')


def tcr_comparison(input_file: str, output_dir: Path):
    """TCR结果比较, 原始数据质量和特定标志物比较"""
    with open(input_file, 'r') as f:
        old_dir, new_dir = f.readline().strip().split(',')
    # * 数据质量
    # reads数, 总碱基数, Q30, 质量好的reads, 质量好的比例. 转长格式方便比对
    olddf = pd.read_csv(f'{old_dir}/automrd_qc_info.tsv', sep='\t',
                        usecols=['Sample', 'Total_reads', 'Total_bases', 'Effective_reads', 'Effective_rate', 'Q30']
                        ).melt(id_vars='Sample', var_name='Stats', value_name='ValueOld')
    newdf = pd.read_csv(f'{new_dir}/automrd_qc_info.tsv', sep='\t',
                        usecols=['Sample', 'Total_reads', 'Total_bases', 'Effective_reads', 'Effective_rate', 'Q30']
                        ).melt(id_vars='Sample', var_name='Stats', value_name='ValueNew')
    merged = pd.merge(olddf, newdf, on=['Sample', 'Stats'], how='left')
    # 删除NTC和POS样本
    merged = merged[~merged['Sample'].str.contains('NTC|POS', case=False, na=False, regex=True)]
    merged['Diff'] = merged.apply(lambda row: row['ValueOld'] - row['ValueNew'], axis=1)
    merged.to_excel(f'{output_dir}/tcr-data-quality.xlsx', index=False)
    # * 特定标志物
    res = list(Path(old_dir).glob('*/stats/trb.top10.tsv'))
    dfs = []
    for tfile in res:
        # 跳过POS和NTC样本
        if re.search(r'POS|NTC', str(tfile), re.IGNORECASE):
            continue
        df_old = pd.read_csv(tfile, sep='\t', usecols=['cdr3', 'num', 'freq']
                             ).melt(id_vars='cdr3', var_name='Stats', value_name='ValueOld')
        df_new = pd.read_csv(str(tfile).replace(old_dir, new_dir), sep='\t', usecols=['cdr3', 'num', 'freq']
                             ).melt(id_vars='cdr3', var_name='Stats', value_name='ValueNew')
        merged = pd.merge(df_old, df_new, on=['cdr3', 'Stats'], how='left').fillna(0)
        merged.insert(0, 'Sample', tfile.parts[-3])
        merged['Diff'] = merged.apply(lambda row: row['ValueNew'] - row['ValueOld'], axis=1)
        dfs.append(merged)
    all_df = pd.concat(dfs, ignore_index=True)
    all_df.to_excel(f'{output_dir}/trb.xlsx', index=False)


def lvis_comparison(input_file: str, output_dir: Path):
    with open(input_file, 'r') as f:
        old_dir, new_dir = f.readline().strip().split(',')
    # * 数据质量
    # reads数, 总碱基数, Q30, 质量好的reads, 质量好的比例. 转长格式方便比对
    olddf = pd.read_csv(f'{old_dir}/qc/fastp/fastp.stats.tsv', sep='\t',
                        usecols=['Sample', 'RawReads', 'RawBases', 'CleanQ20', 'CleanQ30',
                                 'GC', 'CleanReads', 'CleansBases', 'CleanBaseRate']
                        ).melt(id_vars='Sample', var_name='Stats', value_name='ValueOld')
    newdf = pd.read_csv(f'{new_dir}/qc/fastp/fastp.stats.tsv', sep='\t',
                        usecols=['Sample', 'RawReads', 'RawBases', 'CleanQ20', 'CleanQ30',
                                 'GC', 'CleanReads', 'CleansBases', 'CleanBaseRate']
                        ).melt(id_vars='Sample', var_name='Stats', value_name='ValueNew')
    merged = pd.merge(olddf, newdf, on=['Sample', 'Stats'], how='left')
    # 删除NTC和POS样本
    merged = merged[~merged['Sample'].str.contains('NTC|POS', case=False, na=False, regex=True)]
    merged['Diff'] = merged.apply(lambda row: row['ValueOld'] - row['ValueNew'], axis=1)
    merged.to_excel(f'{output_dir}/lvis-data-quality.xlsx', index=False)
    # * 特定标志物
    res = list(Path(old_dir).glob('anno-qc/*.filter.tsv'))
    dfs = []
    for tfile in res:
        # 跳过POS和NTC样本
        if re.search(r'POS|NTC', str(tfile), re.IGNORECASE):
            continue
        df_old = pd.read_csv(tfile, sep='\t', usecols=['Chrom', 'Start', 'UMIs', 'Depth']
                             ).head(10).melt(id_vars=['Chrom', 'Start'], var_name='Stats', value_name='ValueOld')
        df_new = pd.read_csv(str(tfile).replace(old_dir, new_dir), sep='\t',
                             usecols=['Chrom', 'Start', 'UMIs', 'Depth']
                             ).head(10).melt(id_vars=['Chrom', 'Start'], var_name='Stats', value_name='ValueNew')
        merged = pd.merge(df_old, df_new, on=['Chrom', 'Start', 'Stats'], how='left').fillna(0)
        merged.insert(0, 'Sample', tfile.stem.replace('.filter', ''))
        merged['Diff'] = merged.apply(lambda row: row['ValueNew'] - row['ValueOld'], axis=1)
        dfs.append(merged)
    all_df = pd.concat(dfs, ignore_index=True)
    all_df.to_excel(f'{output_dir}/lvis.xlsx', index=False)


if __name__ == '__main__':
    main()
